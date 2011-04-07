/*
 * Copyright (c) 2009-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.xtreemfs.babudb.pbrpc.GlobalTypes.Chunk;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.transmission.ErrorCode;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

import com.google.protobuf.Message;

/**
 * {@link Operation} to request a {@link Chunk} from the master.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class ChunkOperation extends Operation {

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#
     * getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return ReplicationServiceConstants.PROC_ID_CHUNK;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return Chunk.getDefaultInstance();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          processRequest(org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public void processRequest(Request rq) {
        Chunk chunk = (Chunk) rq.getRequestMessage();
        int length = (int) (chunk.getEnd() - chunk.getStart());
      
        Logging.logMessage(Logging.LEVEL_INFO, this, 
                "%s request received from %s", chunk.toString(), 
                rq.getSenderAddress().toString());
        
        FileChannel channel = null;
        ReusableBuffer payload = null;
        try {
            // get the requested chunk
            channel = new FileInputStream(chunk.getFileName()).getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(length);
            if (channel.read(buffer, chunk.getStart()) != length) 
                throw new Exception(); 
            
            buffer.flip();
            payload = new ReusableBuffer(buffer);
            rq.sendSuccess(ErrorCodeResponse.getDefaultInstance(), payload);
            
        } catch (Exception e) {
            
            if (e.getMessage() == null) {
                Logging.logError(Logging.LEVEL_WARN, this, e);
            } else {
                Logging.logMessage(Logging.LEVEL_INFO, this, 
                        "The requested chunk (%s) is not" +
                        " available anymore, because: %s", 
                        chunk.toString(), e.getMessage());
            }
            rq.sendSuccess(
                    ErrorCodeResponse.newBuilder().setErrorCode(
                            ErrorCode.FILE_UNAVAILABLE).build());
        } finally {
            try {
                if (channel != null) channel.close();
            } catch (IOException e) { /* ignored */ }
            
            if (payload != null) BufferPool.free(payload);
        }
    }
}