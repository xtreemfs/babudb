/*
 * Copyright (c) 2009-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.pbrpc.GlobalTypes.DBFileMetaData;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.DBFileMetaDatas;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN;
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
           
    private final AtomicReference<org.xtreemfs.babudb.lsmdb.LSN>  lastOnView;
    
    private final BabuDBInterface                                 babuInterface;
    
    private final FileIOInterface                                 fileIO;
    
    private final int                                             maxChunkSize;
    
    public LoadOperation(AtomicReference<org.xtreemfs.babudb.lsmdb.LSN> lastOnView, 
            int maxChunkSize, BabuDBInterface babuInterface, FileIOInterface fileIO) {
        
        this.fileIO = fileIO;
        this.maxChunkSize = maxChunkSize;
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
        return org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN.getDefaultInstance();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          processRequest(org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public void processRequest(Request rq) {
        LSN request = (LSN) rq.getRequestMessage();
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "LOAD from %s, by %s", 
                request.toString(), rq.getSenderAddress().toString());
        
        if (new org.xtreemfs.babudb.lsmdb.LSN(request.getViewId(), 
                                              request.getSequenceNo())
                .equals(this.lastOnView.get())) {
            
            rq.sendSuccess(DBFileMetaDatas.getDefaultInstance());
        } else {
            DBFileMetaDatas.Builder result = DBFileMetaDatas.newBuilder();
            result.setMaxChunkSize(maxChunkSize);
            
            synchronized (babuInterface.getDBModificationLock()) {
                synchronized (babuInterface.getCheckpointerLock()) {
                    
                    // add the DB-structure-file metadata
                    result.addDbFileMetadatas(convert(
                            this.fileIO.getConfigFileMetaData()));
                    
                    // add the latest snapshot files for every DB,
                    // if available
                    for (org.xtreemfs.babudb.lsmdb.LSMDatabase.DBFileMetaData md 
                            : this.babuInterface.getAllSnapshotFiles()) {
                        result.addDbFileMetadatas(convert(md));
                    }
                }
            }
            rq.sendSuccess(result.build());
        }
    }
    
    private DBFileMetaData convert(
            org.xtreemfs.babudb.lsmdb.LSMDatabase.DBFileMetaData metaData) {
        return DBFileMetaData.newBuilder()
                .setFileName(metaData.file)
                .setFileSize(metaData.size).build();
    }
}