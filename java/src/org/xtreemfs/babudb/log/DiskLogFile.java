/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Interface for reading on-disk operation logs.
 * 
 * @author bjko
 */
public class DiskLogFile {
    
    protected File            file;
    
    protected FileChannel     channel;
    
    protected FileInputStream fis;
    
    protected Checksum        csumAlgo;
    
    protected ByteBuffer      myInt;
    
    protected LogEntry        next;
    
    public DiskLogFile(String baseDir, LSN logLSN) throws IOException, LogEntryException {
        this(baseDir + DiskLogger.createLogFileName(logLSN.getViewId(), logLSN.getSequenceNo()));
    }
    
    public DiskLogFile(String filename) throws IOException, LogEntryException {
        
        file = new File(filename);
        fis = new FileInputStream(file);
        channel = fis.getChannel();
        myInt = ByteBuffer.allocate(Integer.SIZE / 8);
        csumAlgo = new CRC32();
        
        next = getNext();
    }
    
    public void close() throws IOException {
        LogEntry tmp = next;
        next = null;
        if (tmp != null) tmp.free();
        channel.close();
        fis.close();
    }
    
    public boolean hasNext() {
        return next != null;
    }
    
    public LogEntry next() throws LogEntryException {
        LogEntry tmp = next;
        next = getNext();
        return tmp;
    }
    
    protected LogEntry getNext() throws LogEntryException {
        
        long offset = -1;
        ReusableBuffer item = null;
        try {
            
            if (channel.position() == channel.size())
                return null;
            
            int numRead = channel.read(myInt);
            if (numRead < Integer.SIZE / 8)
                return null;
            
            myInt.flip();
            int entrySize = myInt.getInt();
            myInt.flip();
            offset = channel.position() - Integer.SIZE / 8;
            channel.position(offset);
            
            if (entrySize < 0)
                throw new LogEntryException("log entry with negative size detected: " + entrySize);
            
            item = BufferPool.allocate(entrySize);
            channel.read(item.getBuffer());
            item.flip();
            LogEntry e = LogEntry.deserialize(item, csumAlgo);
            csumAlgo.reset();
            return e;
            
        } catch (LogEntryException ex) {
            
            // in case of an invalid log entry ...
            Logging.logMessage(Logging.LEVEL_ERROR, this, "***** INVALID LOG ENTRY *****");
            Logging.logMessage(Logging.LEVEL_ERROR, this,
                "the log contains an invalid log entry at offset %d, file will be truncated at offset %d",
                offset, offset);
            Logging.logMessage(Logging.LEVEL_ERROR, this, ex.getMessage());
            
            // trucate the log at the end of the previous entry
            try {
                
                // close the channel
                channel.close();
                
                // truncate the file
                FileOutputStream fout = new FileOutputStream(file, true);
                fout.getChannel().truncate(offset);
                fout.close();
                
                // re-open the channel and set the position behind the last
                // entry
                fis = new FileInputStream(file);
                channel = fis.getChannel();
                channel.position(offset);
                
            } catch (IOException exc) {
                throw new LogEntryException("Cannot truncate log file: " + ex);
            }
            
            return null;
            
        } catch (IOException ex) {
            
            Logging.logMessage(Logging.LEVEL_DEBUG, this, ex.getMessage());
            throw new LogEntryException("Cannot read log entry: " + ex);
            
        } finally {
            if (item != null)
                BufferPool.free(item);
        }
        
    }
    
}
