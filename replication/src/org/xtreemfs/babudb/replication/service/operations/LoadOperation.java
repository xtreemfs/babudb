/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.DBFileMetaDatas;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.logging.Logging;

import com.google.protobuf.Message;

/**
 * {@link Operation} to request a {@link DBFileMetaDataSet} from the master.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class LoadOperation extends Operation {
           
    private final AtomicReference<LSN>  lastOnView;
    
    private final BabuDBInterface       babuInterface;
    
    private final FileIOInterface       fileIO;
    
    public LoadOperation(AtomicReference<LSN> lastOnView, 
            BabuDBInterface babuInterface, FileIOInterface fileIO) {
        
        this.fileIO = fileIO;
        this.babuInterface = babuInterface;
        this.lastOnView = lastOnView;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#
     * getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return ReplicationServiceConstants.PROC_ID_LOAD;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return LSN.getDefaultInstance();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#
     * startInternalEvent(java.lang.Object[])
     */
    @Override
    public void startInternalEvent(Object[] args) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#
     * startRequest(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public void startRequest(Request rq) {
        DBFileMetaDatas.Builder result = DBFileMetaDatas.newBuilder();
        LSN request = (LSN) rq.getRequestMessage();
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "LOAD from %s, by %s", 
                request.toString(), rq.getRPCRequest()
                .getSenderAddress().toString());
        
        if (new org.xtreemfs.babudb.lsmdb.LSN(request.getViewId(), 
                                              request.getSequenceNo())
                .equals(this.lastOnView.get())) {
            
            rq.sendSuccess(DBFileMetaDatas.getDefaultInstance());
        } else {
            synchronized (this.babuInterface.getDBModificationLock()) {
                synchronized (this.babuInterface.getCheckpointerLock()) {
                    
                    // add the DB-structure-file metadata
                    result.addDbFileMetadatas(
                            this.fileIO.getConfigFileMetaData());
                    
                    // add the latest snapshot files for every DB,
                    // if available
                    result.addAllDbFileMetadatas(
                            this.babuInterface.getAllSnapshotFiles());
                }
            }
            rq.sendSuccess(result.build());
        }
    }
}