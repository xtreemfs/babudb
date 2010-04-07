/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import static org.xtreemfs.babudb.log.LogEntry.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.clients.MasterClient;
import org.xtreemfs.babudb.clients.StateClient;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.foundation.oncrpc.client.RPCResponse;

/**
 * Static functions used in more than one {@link Logic}.
 * 
 * @author flangner
 * @since 06/08/2009
 */

public final class SharedLogic {
    
    /**
     * <p>
     * Retrieve the information from a {@link LogEntry} to replicate it's operation
     * to the {@link BabuDB}.
     * !!!The entry will not be freed here!!!
     * </p>
     * 
     * @param entry - the {@link LogEntry}.
     * @param listener - will be notified, if entry was replicated. 
     *                                      
     * @param dbs - the {@link BabuDB} database system.
     * @throws BabuDBException 
     * @throws InterruptedException the entry will not be written, if thrown.
     */
    static void handleLogEntry(LogEntry entry, SyncListener listener, BabuDB dbs) 
            throws BabuDBException, InterruptedException {
        DatabaseManagerImpl dbMan = (DatabaseManagerImpl) dbs.getDatabaseManager();

        // check the payload type
        switch (entry.getPayloadType()) { 
        
        case PAYLOAD_TYPE_INSERT:
            InsertRecordGroup irg = InsertRecordGroup.deserialize(entry.getPayload());
            dbMan.insert(irg);
            break;
        
        case PAYLOAD_TYPE_CREATE:
            // deserialize the create call
            int dbId = entry.getPayload().getInt();
            String dbName = entry.getPayload().getString();
            int indices = entry.getPayload().getInt();
            if (dbMan.getDatabase(dbId) == null)
                dbMan.proceedCreate(dbName, indices, null);
            break;
            
        case PAYLOAD_TYPE_COPY:
            // deserialize the copy call
            entry.getPayload().getInt(); // do not delete!
            dbId = entry.getPayload().getInt();
            String dbSource = entry.getPayload().getString();
            dbName = entry.getPayload().getString();
            if (dbMan.getDatabase(dbId) == null)
                dbMan.proceedCopy(dbSource, dbName);
            break;
            
        case PAYLOAD_TYPE_DELETE:
            // deserialize the create operation call
            dbId = entry.getPayload().getInt();
            dbName = entry.getPayload().getString();
            if (dbMan.getDatabase(dbId) != null)
                dbMan.proceedDelete(dbName);
            break;
            
        case PAYLOAD_TYPE_SNAP:
            ObjectInputStream oin = null;
            try {
                oin = new ObjectInputStream(new ByteArrayInputStream(
                        entry.getPayload().array()));
                // deserialize the snapshot configuration
                dbId = oin.readInt();
                SnapshotConfig snap = (SnapshotConfig) oin.readObject();
                ((SnapshotManagerImpl) dbs.getSnapshotManager()).
                    createPersistentSnapshot(dbMan.getDatabase(dbId).
                            getName(),snap, false);
            } catch (Exception e) {
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,
                        "Could not deserialize operation of type "+entry.
                        getPayloadType()+", because: "+e.getMessage(), e);
            } finally {
                try {
                if (oin != null) oin.close();
                } catch (IOException ioe) {
                    /* who cares? */
                }
            }
            break;
            
        case PAYLOAD_TYPE_SNAP_DELETE:

            byte[] payload = entry.getPayload().array();
            int offs = payload[0];
            dbName = new String(payload, 1, offs);
            String snapName = new String(payload, offs + 1, payload.length - offs - 1);
            
            ((SnapshotManagerImpl) dbs.getSnapshotManager())
                    .deletePersistentSnapshot(dbName, snapName, false);
            break;
                
        default: new BabuDBException(ErrorCode.INTERNAL_ERROR,
                "unknown payload-type");
        }
        entry.getPayload().flip();
        entry.setListener(listener);
        
        // append logEntry to the logFile
        dbs.getLogger().append(entry);  
    }
    
    /**
     * Method to synchronously switch the log file.
     * 
     * @throws InterruptedException 
     * @throws IOException
     * @return the {@link LSN} of the last synchronized {@link LogEntry}.  
     */
    static LSN switchLogFile(DiskLogger logger) throws InterruptedException, IOException {
        try {
            logger.lockLogger();
            return new LSN(logger.switchLogFile(true).getViewId()+1, 1L);
        } finally {
            logger.unlockLogger();
        }
    }
    
    /**
     * Chooses a server from the given list which is at least that up-to-date to
     * synchronize with this server.
     * 
     * @param progressAtLeast
     * @param babuDBS
     * @param client
     * @param master - client for the replications master.
     * @return a master client retrieved from the list, or null if none of the 
     *         given babuDBs has the required information. 
     */
    static MasterClient getSynchronizationPartner(Set<InetSocketAddress> babuDBS, 
            LSN progressAtLeast, MasterClient master) {
        InetSocketAddress local = master.getLocalAddress();
        
        List<InetSocketAddress> servers = 
            new LinkedList<InetSocketAddress>(babuDBS);
        servers.remove(master.getDefaultServerAddress());
        
        if (servers.size() > 0) {
            Map<InetSocketAddress, LSN> states = getStates(servers, 
                    master.getClient(), local);
            
            for (Entry<InetSocketAddress, LSN> e : states.entrySet()) {
                if (e.getValue().compareTo(progressAtLeast) >= 0)
                    return new MasterClient(master.getClient(), e.getKey(), local);
            }
        }
        
        return master;
    }
    
    /**
     * <p>
     * Performs a network broadcast to get the latest LSN from every available DB.
     * </p>
     * 
     * @param babuDBs
     * @param client
     * @param localAddress
     * @return the LSNs of the latest written LogEntries for the <code>babuDBs
     *         </code> that were available.
     */
    @SuppressWarnings("unchecked")
    public static Map<InetSocketAddress, LSN> getStates(
            List<InetSocketAddress> babuDBs, RPCNIOSocketClient client, 
            InetSocketAddress localAddress) {
        int numRqs = babuDBs.size();
        Map<InetSocketAddress,LSN> result = new HashMap<InetSocketAddress, LSN>();
        RPCResponse<LSN>[] rps = new RPCResponse[numRqs];
        
        // send the requests
        StateClient c;
        for (int i=0;i<numRqs;i++) {
            c = new StateClient(client, babuDBs.get(i), localAddress);
            rps[i] = (RPCResponse<LSN>) c.getState();
        }
        
        // get the responses
        for (int i=0;i<numRqs;i++) {
            try{
                LSN val = rps[i].get();
                result.put(babuDBs.get(i), val);
            } catch (Exception e) {
                Logging.logMessage(Logging.LEVEL_INFO, client, 
                        "Could not receive state of '%s', because: %s.", 
                        babuDBs.get(i), e.getMessage());
                if (e.getMessage() == null)
                    Logging.logError(Logging.LEVEL_INFO, client, e);
            } finally {
                if (rps[i]!=null) rps[i].freeBuffers();
            }
        }
        
        return result;
    }
}