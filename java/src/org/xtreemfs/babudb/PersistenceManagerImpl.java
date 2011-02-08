/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import java.util.HashMap;
import java.util.Map;

import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.InMemoryProcessing;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * Default implementation of the {@link PersistenceManager} interface using
 * the {@link DiskLogger} to let operations become persistent.
 * 
 * @author flangner
 * @since 11/03/2010
 */
class PersistenceManagerImpl implements PersistenceManager {

    private final Map<Byte, InMemoryProcessing> inMemoryProcessing = 
        new HashMap<Byte, InMemoryProcessing>();
    
    private DiskLogger diskLogger;
    
    private volatile LSN latestOnDisk;
        
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#init(org.xtreemfs.babudb.lsmdb.LSN)
     */
    public void init(LSN initial) {
        this.latestOnDisk = initial; 
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#setLogger(
     *          org.xtreemfs.babudb.log.DiskLogger)
     */
    public void setLogger(DiskLogger logger) {
        this.diskLogger = logger;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#makePersistent(byte, java.lang.Object[])
     */
    @Override
    public <T> DatabaseRequestResult<T> makePersistent(byte type, Object[] args) 
            throws BabuDBException {
        
        return makePersistent(type, args, inMemoryProcessing.get(type).serializeRequest(args));
    }
      
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#makePersistent(byte, java.lang.Object[], 
     *          org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @Override
    public <T> DatabaseRequestResult<T> makePersistent(byte type, final Object[] args, 
            ReusableBuffer payload) throws BabuDBException {
        
        final InMemoryProcessing processing = inMemoryProcessing.get(type);
        
        processing.before(args);
        
        // build the entry
        LogEntry entry = new LogEntry(payload, null, type);
        
        // setup the result
        final BabuDBRequestResultImpl<T> result =
            new BabuDBRequestResultImpl<T>(entry);
        
        // define the listener
        entry.setListener(new SyncListener() {
            
            @Override
            public void synced(LogEntry entry) {
                if (entry != null) entry.free();
                processing.after(args);
                result.finished();
            }
            
            @Override
            public void failed(LogEntry entry, Exception ex) {
                if (entry != null) entry.free();
                result.failed((ex != null && ex instanceof BabuDBException) ? 
                        (BabuDBException) ex : new BabuDBException(
                                ErrorCode.INTERNAL_ERROR, ex.getMessage()));
            }
        });
        
        // append the entry to the DiskLogger
        try {
            this.diskLogger.append(entry);
        } catch (InterruptedException ie) {
            if (entry != null) entry.free();
            throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "Operation " +
                        "could not have been stored persistent to disk an " +
                        "will therefore be discarded.", ie.getCause());
        } 
        
        return result;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#lockService()
     */
    @Override
    public void lockService() throws InterruptedException {
        if(this.diskLogger != null)
            this.diskLogger.lockLogger();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#unlockService()
     */
    @Override
    public void unlockService() {
        if (this.diskLogger != null && this.diskLogger.hasLock())
            this.diskLogger.unlockLogger();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#registerInMemoryProcessing(byte, 
     *          org.xtreemfs.babudb.api.InMemoryProcessing)
     */
    @Override
    public void registerInMemoryProcessing(byte type, InMemoryProcessing processing) {
        this.inMemoryProcessing.put(type, processing);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#getLatestOnDiskLSN()
     */
    @Override
    public LSN getLatestOnDiskLSN() {
        return latestOnDisk;
    }
}
