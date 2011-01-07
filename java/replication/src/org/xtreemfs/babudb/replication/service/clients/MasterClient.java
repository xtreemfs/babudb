/*
 * Copyright (c) 2010-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.clients;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.logic.LoadLogic.DBFileMetaDataSet;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * Client to access services of a server that has an active master lease.
 *
 * @author flangner
 * @since 04/12/2010
 */
public interface MasterClient extends ConditionClient {

    /**
     * Requests a list of serialized {@link LogEntry}s inclusive between the 
     * given {@link LSN}s start and end at the master.
     * 
     * @param start
     * @param end
     * @return the {@link ClientResponseFuture} to receive a list of serialized 
     *         LogEntries.
     */
    public ClientResponseFuture<ReusableBuffer[]> replica(LSN start, LSN end);

    /**
     * Requests the chunk data with the given chunk details at the master.
     * 
     * @param fileName
     * @param start
     * @param end
     * @return the {@link ClientResponseFuture} for receiving Chunk-data.
     */
    public ClientResponseFuture<ReusableBuffer> chunk(String fileName, 
                                                      long start, long end);
    
    /**
     * Requests the DBFileMetadata of the master.
     * 
     * @param lsn - of the latest written {@link LogEntry}.
     * @return the {@link ClientResponseFuture} receiving a 
     *         {@link DBFileMetaDataSet}.
     */ 
    public ClientResponseFuture<DBFileMetaDataSet> load(LSN lsn);
}