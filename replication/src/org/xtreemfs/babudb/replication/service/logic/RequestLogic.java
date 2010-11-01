/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.logic;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.interfaces.LogEntries;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.errnoException;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.service.Pacemaker;
import org.xtreemfs.babudb.replication.service.ReplicationStage;
import org.xtreemfs.babudb.replication.service.SlaveView;
import org.xtreemfs.babudb.replication.service.ReplicationStage.ConnectionLostException;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.ErrNo;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.foundation.oncrpc.utils.ONCRPCException;

import static org.xtreemfs.babudb.replication.service.logic.LogicID.*;
/**
 * <p>Requests missing {@link LogEntry}s at the 
 * master and inserts them into the DBS.</p>
 * 
 * @author flangner
 * @since 06/08/2009
 */

public class RequestLogic extends Logic {
    
    /** checksum algorithm used to deserialize logEntries */
    private final Checksum              checksum = new CRC32();

    /**
     * @param stage
     * @param pacemaker
     * @param slaveView
     * @param fileIO
     * @param babuInterface
     */
    public RequestLogic(ReplicationStage stage, Pacemaker pacemaker, 
            SlaveView slaveView, FileIOInterface fileIO, 
            BabuDBInterface babuInterface) {
        super(stage, pacemaker, slaveView, fileIO, babuInterface);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.stages.logic.Logic#getId()
     */
    @Override
    public LogicID getId() {
        return REQUEST;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() throws InterruptedException, ConnectionLostException{
        LSN lsnAtLeast = new LSN(stage.missing.getEnd());
        
        Logging.logMessage(Logging.LEVEL_INFO, this, 
                "Replica-range is missing: from %s to %s", 
                new LSN(stage.missing.getStart()).toString(),
                lsnAtLeast.toString());
        
        // get the missing logEntries
        RPCResponse<LogEntries> rp = null;    
        LogEntries logEntries = null;
        
        
        MasterClient master = this.slaveView.getSynchronizationPartner(
                lsnAtLeast);
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "Replica-Range will be" +
        		" retrieved from %s.", master.getDefaultServerAddress());
        
        try {
            rp = master.replica(stage.missing);
            logEntries = (LogEntries) rp.get();
            
            // enhancement if the request had detected a master-failover
            if (logEntries.size() == 0) {
                this.stage.lastOnView.set(this.babuInterface.getState());
                this.babuInterface.checkpoint();
                this.stage.lastInserted = this.babuInterface.getState();
                finish();
                return;
            }
            
            final AtomicInteger count = new AtomicInteger(logEntries.size());
            // insert all logEntries
            LSN check = null;
            for (org.xtreemfs.babudb.interfaces.LogEntry le : logEntries) {
                try {
                    final LogEntry logentry = 
                        LogEntry.deserialize(le.getPayload(), this.checksum);
                    final LSN lsn = logentry.getLSN();
                    
                    // assertion whether the received entry does match the order 
                    // or not
                    assert (check == null || 
                           (check.getViewId() == lsn.getViewId() && 
                            check.getSequenceNo()+1L == lsn.getSequenceNo()) ||
                            check.getViewId()+1 == lsn.getViewId() &&
                            lsn.getSequenceNo() == 1L) : "ERROR: last LSN (" +
                            check.toString() + ") received LSN (" + 
                            lsn.toString() + ")!";
                    check = lsn;
                    
                    // we have to switch the log-file
                    if (lsn.getSequenceNo() == 1L && 
                        stage.lastInserted.getViewId() < lsn.getViewId()) {
                        
                        this.stage.lastOnView.set(this.babuInterface.getState());
                        this.babuInterface.checkpoint();
                        stage.lastInserted = this.babuInterface.getState();
                    }
                    
                    this.slaveView.handleLogEntry(logentry, new SyncListener() {
                    
                        @Override
                        public void synced(LogEntry entry) {
                            synchronized (count) {
                                stage.lastInserted = lsn;
                                if (count.decrementAndGet() == 0)
                                    count.notify();
                            }
                            entry.free();
                        }
                    
                        @Override
                        public void failed(LogEntry entry, Exception ex) {
                            Logging.logError(Logging.LEVEL_ERROR, stage, ex);
                            synchronized (count) {
                                count.set(-1);
                                count.notify();
                            }
                            entry.free();
                        }
                    });
                } finally {
                    this.checksum.reset();
                }  
            }
            
            // block until all inserts are finished
            synchronized (count) {
                while (count.get() > 0)
                    count.wait();
            }
            
            // at least one insert failed
            if (count.get() == -1) 
                throw new LogEntryException("At least one insert could not be" +
                		" proceeded.");
 
            finish();
            if (Thread.interrupted()) 
                throw new InterruptedException("Replication was interrupted" +
                		" after executing a replicaOperation.");
        } catch (errnoException e) {
            // server-side error
            throw new ConnectionLostException(e.getError_message(), 
                                              e.getError_code());
        } catch (ONCRPCException e) {
            // remote failure (connection lost)
            throw new ConnectionLostException(e.getTypeName() + ": " + 
                                              e.getMessage(), ErrNo.UNKNOWN);
        } catch (IOException ioe) {
            // failure on transmission (connection lost or request timed out)
            throw new ConnectionLostException(ioe.getMessage(), ErrNo.BUSY);
        } catch (LogEntryException lee) {
            // decoding failed --> retry with new range
            Logging.logError(Logging.LEVEL_WARN, this, lee);
            finish();
        } catch (BabuDBException be) {
            // the insert failed due an DB error
            Logging.logError(Logging.LEVEL_WARN, this, be);
            stage.missing = new LSNRange(
                    new org.xtreemfs.babudb.interfaces.LSN(
                            stage.lastInserted.getViewId(),
                            stage.lastInserted.getSequenceNo()), 
                    stage.missing.getEnd());
            stage.setLogic(LOAD, be.getMessage());
        } finally {
            if (rp!=null) rp.freeBuffers();
            if (logEntries!=null) 
                for (org.xtreemfs.babudb.interfaces.LogEntry le : logEntries) 
                    if (le.getPayload()!=null) BufferPool.free(le.getPayload());
        }
    }
    
    /**
     * Method to decide which logic shall be used next.
     */
    private void finish() {
        LSN next = new LSN (stage.lastInserted.getViewId(),
                            stage.lastInserted.getSequenceNo() + 1L);
        
        // we are still missing some entries (the request was too large)
        // update the missing entries
        if (next.compareTo(new LSN (stage.missing.getEnd())) < 0) {
            stage.missing = new LSNRange(
                    new org.xtreemfs.babudb.interfaces.LSN(
                            stage.lastInserted.getViewId(),
                            stage.lastInserted.getSequenceNo()), 
                    stage.missing.getEnd());
         // all went fine --> back to basic
        } else {
            stage.missing = null;
            stage.setLogic(BASIC, "Request went fine, we can go on with" +
                        " the basicLogic.");
        }
    }
}