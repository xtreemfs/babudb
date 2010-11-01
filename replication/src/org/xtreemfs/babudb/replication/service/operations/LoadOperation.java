/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.interfaces.DBFileMetaDataSet;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.loadRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.loadResponse;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.logging.Logging;

/**
 * {@link Operation} to request a {@link DBFileMetaDataSet} from the master.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class LoadOperation extends Operation {

    private final int                   procId;
           
    private final AtomicReference<LSN>  lastOnView;
    
    private final BabuDBInterface       babuInterface;
    
    private final FileIOInterface       fileIO;
    
    public LoadOperation(AtomicReference<LSN> lastOnView, 
            BabuDBInterface babuInterface, FileIOInterface fileIO) {
        
        this.fileIO = fileIO;
        this.babuInterface = babuInterface;
        this.lastOnView = lastOnView;
        this.procId = new loadRequest().getTag();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return this.procId;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#parseRPCMessage(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public yidl.runtime.Object parseRPCMessage(Request rq) {
        loadRequest rpcrq = new loadRequest();
        rq.deserializeMessage(rpcrq);
        
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#startInternalEvent(java.lang.Object[])
     */
    @Override
    public void startInternalEvent(Object[] args) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#startRequest(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public void startRequest(Request rq) {
        DBFileMetaDataSet result = new DBFileMetaDataSet();
        loadRequest request = (loadRequest) rq.getRequestMessage();
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "LOAD from %s, by %s", 
                request.getLsn().toString(), rq.getRPCRequest()
                .getClientIdentity().toString());
        
        if (new LSN(request.getLsn()).equals(this.lastOnView.get())) {
            rq.sendSuccess(new loadResponse());
        } else {
            synchronized (this.babuInterface.getDBModificationLock()) {
                synchronized (this.babuInterface.getCheckpointerLock()) {
                    
                    // add the DB-structure-file metadata
                    result.add(this.fileIO.getConfigFileMetaData());
                    
                    // add the latest snapshot files for every DB,
                    // if available
                    result.addAll(this.babuInterface.getAllSnapshotFiles());
                }
            }
            rq.sendSuccess(new loadResponse(result));
        }
    }
}