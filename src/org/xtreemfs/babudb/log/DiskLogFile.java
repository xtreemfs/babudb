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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.common.buffer.BufferPool;
import org.xtreemfs.common.buffer.ReusableBuffer;
import org.xtreemfs.common.logging.Logging;

/**
 * Interface for reading on-disk operation logs.
 * @author bjko
 */
public class DiskLogFile {
    
    protected File            file;
    
    protected FileChannel     channel;
    
    protected FileInputStream fis;
    
    protected Checksum        csumAlgo;
    
    protected ByteBuffer      myInt;
   
    public DiskLogFile(String baseDir, LSN logLSN) throws IOException {
        this(baseDir+DiskLogger.createLogFileName(logLSN.getViewId(), logLSN.getSequenceNo()));
    }
    
    public DiskLogFile(String filename) throws IOException {
        file = new File(filename);
        fis = new FileInputStream(file);
        channel = fis.getChannel();
        myInt = ByteBuffer.allocate(Integer.SIZE/8);
        csumAlgo = new CRC32();
    }
    
    public void close() throws IOException {
        channel.close();
        fis.close();
    }
    
    public boolean hasNext() {
        try {
            return (channel.size()-channel.position()) > 0;
        } catch (IOException ex) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this,ex);
            return false;
        }
    }
    
    public LogEntry next() throws LogEntryException {
        ReusableBuffer item = null;
        try {
            int numRead = channel.read(myInt);
            if (numRead < Integer.SIZE/8)
                return null;
            myInt.flip();
            int entrySize = myInt.getInt();
            myInt.flip();
            item = BufferPool.allocate(entrySize);
            channel.position(channel.position()-Integer.SIZE/8);
            channel.read(item.getBuffer());
            item.flip();
            LogEntry e = LogEntry.deserialize(item,csumAlgo);
            csumAlgo.reset();
            BufferPool.free(item);
            return e;
        } catch (IOException ex) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this,ex);
            BufferPool.free(item);
            throw new LogEntryException("Cannot read log entry: "+ex);
        }
    }
    
}
