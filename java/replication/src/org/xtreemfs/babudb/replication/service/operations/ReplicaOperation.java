/*
 * Copyright (c) 2009-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.log.DiskLogIterator;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.LSNRange;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.LogEntries;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.transmission.ErrorCode;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

import com.google.protobuf.Message;

/**
 * {@link Operation} to request {@link LogEntry}s from the master.
 * This operation tries to retrieve the logEntries from the log by iterating
 * over them and returning the requested ones.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class ReplicaOperation extends Operation {

    private final static int            MAX_LOGENTRIES_PER_REQUEST = 100;
            
    private final Checksum              checksum = new CRC32();
    
    private final AtomicReference<LSN>  lastOnView;
    
    private final BabuDBInterface       babuInterface;
    
    private final FileIOInterface       fileIO;
    
    public ReplicaOperation(AtomicReference<LSN> lastOnView, 
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
        return ReplicationServiceConstants.PROC_ID_REPLICA;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     * getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return LSNRange.getDefaultInstance();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#processRequest(org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public void processRequest(Request rq) {
        LSNRange request = (LSNRange) rq.getRequestMessage();
        
        final LSN start = new LSN(request.getStart().getViewId(), 
                                  request.getStart().getSequenceNo());
        final LSN end = new LSN(request.getEnd().getViewId(), 
                                request.getEnd().getSequenceNo());
        
        LogEntries.Builder result = LogEntries.newBuilder();
        ReusableBuffer resultPayLoad = BufferPool.allocate(0);
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "REQUEST received " +
                "(start: %s, end: %s) from %s", start.toString(), 
                end.toString(), rq.getSenderAddress().toString());
        
        // enhancement to prevent slaves from loading the DB from the master unnecessarily
        if (start.equals(lastOnView.get())) {
            
            Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                   "REQUEST answer is empty (there has been a failover only).");
            
            rq.sendSuccess(result.build());
            return;  
        }
        
        final LSN firstEntry = new LSN(start.getViewId(), 
                start.getSequenceNo() + 1L);
        
        assert (firstEntry.compareTo(end) <= 0) : 
            "At least one LogEntry has to be requested!";
        
        DiskLogIterator it = null;
        LogEntry le = null;
        synchronized (babuInterface.getCheckpointerLock()) {
            try {   
                // wait, if there is a checkpoint in proceeding
                babuInterface.waitForCheckpoint();
                
                try {
                    it = fileIO.getLogEntryIterator(firstEntry);
                } catch (LogEntryException exc) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "LogEntryIterator for LSN(%s) is unavailable.", 
                            firstEntry.toString()); 
                    Logging.logError(Logging.LEVEL_DEBUG, this, exc); 
                    if (resultPayLoad != null) BufferPool.free(resultPayLoad);
                    rq.sendSuccess(result.setErrorCode(ErrorCode.LOG_UNAVAILABLE).build());
                    return;
                }
                while (it.hasNext() &&
                       result.getLogEntriesCount() < 
                       MAX_LOGENTRIES_PER_REQUEST && (le = it.next())
                           .getLSN().compareTo(end) <= 0) {
                    
                    try {
                        // we are not at the right position yet -> skip
                        if (le.getLSN().compareTo(firstEntry) < 0) continue;
                    
                        // the first entry was not available ... bad
                        if (le.getLSN().compareTo(firstEntry) > 0 &&
                            result.getLogEntriesCount() == 0) {
                            break;
                        }
                          
                        // add the logEntry to result list
                        assert (le.getPayload().array().length > 0) : 
                            "Empty log-entries are not allowed!";
                        ReusableBuffer buf = le.serialize(checksum);
                        
                        result.addLogEntries(
                                org.xtreemfs.babudb.pbrpc.GlobalTypes.LogEntry
                                .newBuilder().setLength(buf.remaining()));
                        
                        int newSize = resultPayLoad.position() + buf.remaining();
                        
                        if (!resultPayLoad.enlarge(newSize)) {
                            ReusableBuffer tmp = BufferPool.allocate(newSize);
                            
                            tmp.put(resultPayLoad);
                            BufferPool.free(resultPayLoad);
                            resultPayLoad = tmp;
                            assert (resultPayLoad.remaining() >= buf.remaining()) :
                                "the target buffer (" + resultPayLoad.remaining() + ") has " +
                                "not enough space allocated to fetch the src buffer (" + 
                                buf.remaining() + ")";
                            
                        }
                        assert (resultPayLoad.remaining() >= buf.remaining()) :
                            "the target buffer (" + resultPayLoad.remaining() + ") was not " +
                            "correctly enlarged to fetch the src buffer (" + buf.remaining() +
                            "), newSize: " + newSize;
                        
                        resultPayLoad.put(buf);
                        BufferPool.free(buf);
                        
                    } finally {
                        checksum.reset();
                        if (le != null) {
                            le.free();
                            le = null;
                        }
                    }
                } 
                
                if (result.getLogEntriesCount() > 0) {
                    // send the response, if the requested log entries are found
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "REQUEST: returning %d log-entries to %s.", 
                            result.getLogEntriesCount(), rq.getSenderAddress().toString()); 
                    
                    resultPayLoad.flip();
                    rq.sendSuccess(result.build(), resultPayLoad);
                } else {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "No logentries could have been retrieved.");
                    rq.sendSuccess(result.setErrorCode(ErrorCode.LOG_UNAVAILABLE).build());
                }
            } catch (Exception e) {
                Logging.logError(Logging.LEVEL_INFO, this, e);
                
                // clear result
                BufferPool.free(resultPayLoad);
                
                rq.sendSuccess(result.setErrorCode(ErrorCode.BUSY).build());
            } finally {
                if (le != null) le.free();
                
                if (it != null) {
                    try {
                        it.destroy();
                    } catch (IOException e) { /* ignored */ }
                }
            }
        }
    }
}