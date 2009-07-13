/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.log;

import java.io.IOException;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;

/**
 *
 * @author bjko
 */
public class LogEntry {

    /**
     * length of the entry's header (excluding the length field itself)
     */
    protected static final int headerLength = Integer.SIZE/8*4+Long.SIZE/8;
    
    public static final boolean USE_CHECKSUMS = true;
    
    /**
     * view ID of the log entry. The view ID is an epoch number which
     * creates a total order on the log entries (viewId.logSequenceNo).
     */
    protected int             viewId = -1;
    
    protected long            logSequenceNo = -1L;
    
    protected int             checksum;
    
    protected ReusableBuffer  payload;
    
    protected SyncListener    listener;
    
    private   LSMDBRequest    attachment;
    
    
    private LogEntry() {
    }
    
    public LogEntry(ReusableBuffer payload, SyncListener l) {
        this.payload = payload;
        this.listener = l;
    }
    
    
    public void assignId(int viewId, long logSequenceNo) {
        this.viewId = viewId;
        this.logSequenceNo = logSequenceNo;
    }
    
    
    public ReusableBuffer serialize(Checksum csumAlgo) throws IOException {
        assert(viewId > 0);
        assert(logSequenceNo > 0);
        
        final int bufSize = headerLength+payload.remaining();
        ReusableBuffer buf = BufferPool.allocate(bufSize);
        buf.putInt(bufSize);
        buf.putInt(checksum);
        buf.putInt(viewId);
        buf.putLong(logSequenceNo);
        buf.put(payload);
        payload.flip(); // otherwise payload is not reusable
        buf.putInt(bufSize);
        buf.flip();
        
        if (USE_CHECKSUMS) {
            // reset the old checksum to 0, before calculating a new one
            buf.position(Integer.SIZE/8);
            buf.putInt(0);
            buf.position(0);
            
            csumAlgo.update(buf.array(),0,buf.limit());
            int cPos = buf.position();
            buf.position(Integer.SIZE/8);
            buf.putInt((int)csumAlgo.getValue());
            buf.position(cPos);
        }
        
        return buf;
    }
    
    public SyncListener getListener() {
        return listener;
    }
    
    public ReusableBuffer getPayload() {
        return this.payload;
    }
    
    public int getViewId() {
        return this.viewId;
    }
    
    public long getLogSequenceNo() {
        return this.logSequenceNo;
    }
    
    public LSN getLSN() {
        return new LSN(this.viewId,this.logSequenceNo);
    }
   
    public static void checkIntegrity(ReusableBuffer data) throws LogEntryException {
        int cPos = data.position();
        
        if (data.remaining() < Integer.SIZE/8)
            throw new LogEntryException("Empty data. Cannot read log entry.");
        
        int length1 = data.getInt();
        
        if ((length1-Integer.SIZE/8) > data.remaining()) {
            data.position(cPos);
            Logging.logMessage(Logging.LEVEL_DEBUG, null,"not long enough");
            throw new LogEntryException("The log entry is incomplete. The length indicated in the header "+
                    "exceeds the available data.");
        }
        
        data.position(cPos+length1-Integer.SIZE/8);
        
        int length2 = data.getInt();
        
        data.position(cPos);
        
        if (length1 != length2) {
            throw new LogEntryException("Invalid Frame. The length entries do not match.");
        }
        
    }
    
    public static LogEntry deserialize(ReusableBuffer data, Checksum csumAlgo) throws LogEntryException {
        checkIntegrity(data);
        
        final int bufSize = data.getInt();
        LogEntry e = new LogEntry(); 
        e.checksum = data.getInt();
        e.viewId = data.getInt();
        e.logSequenceNo = data.getLong();
        final int payloadSize = bufSize - headerLength;
        int payloadPosition = data.position();
        ReusableBuffer payload = data.createViewBuffer();
        payload.range(payloadPosition, payloadSize);
        e.payload = payload;
        
        if (USE_CHECKSUMS) {
            data.position(Integer.SIZE/8);
            data.putInt(0);
            data.position(0);
            csumAlgo.update(data.array(),0,data.limit());

            int csum = (int)csumAlgo.getValue();        

            //Logging.logMessage(Logging.LEVEL_DEBUG, null,"checksum is: "+csum+" expected: "+e.checksum);
            if (csum != e.checksum) {
                throw new LogEntryException("Invalid Checksum. Checksum in log entry and calculated checksum do not match.");
            }
        }
        
        return e;
    }
    
    public void free() {
        BufferPool.free(payload);
        payload = null;
    }

    public LSMDBRequest getAttachment() {
        return attachment;
    }

    public void setAttachment(LSMDBRequest attachment) {
        this.attachment = attachment;
    }
    
    /**
     * <p>Just for slaves with replication issues.</p>
     * 
     * @param listener
     */
    public void setListener(SyncListener listener){
        this.listener = listener;
    }
}
