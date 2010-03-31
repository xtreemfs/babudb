/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import java.io.File;

import org.xtreemfs.babudb.interfaces.DBFileMetaData;
import org.xtreemfs.babudb.interfaces.DBFileMetaDataSet;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.loadRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.loadResponse;
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.Request;
import org.xtreemfs.babudb.replication.RequestDispatcher;
import org.xtreemfs.foundation.logging.Logging;

/**
 * {@link Operation} to request a {@link DBFileMetaDataSet} from the master.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class LoadOperation extends Operation {

    private final int procId;
    private final RequestDispatcher dispatcher;
    
    public LoadOperation(RequestDispatcher dispatcher) {
        this.procId = new loadRequest().getTag();
        this.dispatcher = dispatcher;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return procId;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#parseRPCMessage(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public yidl.runtime.Object parseRPCMessage(Request rq) {
        loadRequest rpcrq = new loadRequest();
        rq.deserializeMessage(rpcrq);
        
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#startInternalEvent(java.lang.Object[])
     */
    @Override
    public void startInternalEvent(Object[] args) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#startRequest(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public void startRequest(Request rq) {
        DBFileMetaDataSet result = new DBFileMetaDataSet();
        loadRequest request = (loadRequest) rq.getRequestMessage();
        
        Logging.logMessage(Logging.LEVEL_DEBUG, dispatcher, "LOAD from %s, by %s", 
                request.getLsn().toString(), rq.getRPCRequest()
                .getClientIdentity().toString());
        
        if (new LSN(request.getLsn().getViewId(),request.getLsn().getSequenceNo())
                .equals(dispatcher.lastOnView.get())) {
            rq.sendSuccess(new loadResponse());
        } else {
            synchronized (((DatabaseManagerImpl) dispatcher.dbs.
                    getDatabaseManager()).getDBModificationLock()) {
                synchronized (((CheckpointerImpl) dispatcher.dbs.
                        getCheckpointer()).getCheckpointerLock()) {
                    // add the DB-structure-file metadata
                    int chunkSize = dispatcher.getConfig().getChunkSize();
                    String path = dispatcher.dbs.getDBConfigPath();
                    assert (path!=null) : "No checkpoint available!";
                    long length = new File(path).length();
                    result.add(new DBFileMetaData(path,length,chunkSize));
                    
                    // add the latest snapshot files for every DB,
                    // if available
                    for (Database db : ((DatabaseManagerImpl) dispatcher.
                            dbs.getDatabaseManager()).getDatabaseList())
                        result.addAll(((DatabaseImpl) db).getLSMDB().
                                getLastestSnapshotFiles(chunkSize));  
                }
            }
            rq.sendSuccess(new loadResponse(result));
        }
    }
}