/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import java.util.concurrent.atomic.AtomicReference;

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
class PersistenceManagerImpl extends PersistenceManager {
    
    private final AtomicReference<DiskLogger> diskLogger = new AtomicReference<DiskLogger>(null);
    
    private volatile LSN latestOnDisk;
        
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#init(org.xtreemfs.babudb.lsmdb.LSN)
     */
    public void init(LSN initial) {
        latestOnDisk = initial; 
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#setLogger(
     *          org.xtreemfs.babudb.log.DiskLogger)
     */
    public void setLogger(DiskLogger logger) {
        
        DiskLogger old = diskLogger.getAndSet(logger);
        if (logger == null) {
            latestOnDisk = old.getLatestLSN();
        }
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
                try {
                    processing.after(args);
                    result.finished();
                } catch (BabuDBException e) {
                    result.failed(e);
                }
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
            DiskLogger logger = diskLogger.get();
            if (logger != null) {
                logger.append(entry);
            } else {
                if (entry != null) entry.free();
                throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "BabuDB has currently been " +
                	"stopped by an internal event. It will be back as soon as possible.");
            }
        } catch (InterruptedException ie) {
            if (entry != null) entry.free();
            throw new BabuDBException(ErrorCode.INTERRUPTED, "Operation " +
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
        DiskLogger logger = diskLogger.get();
        if(logger != null) {
            logger.lockLogger();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#unlockService()
     */
    @Override
    public void unlockService() {
        
        DiskLogger logger = diskLogger.get();
        if (logger != null && logger.hasLock()) {
            logger.unlockLogger();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#getLatestOnDiskLSN()
     */
    @Override
    public LSN getLatestOnDiskLSN() {
        DiskLogger logger = diskLogger.get();
        if (logger != null) {
            return logger.getLatestLSN();
        } else {
            return latestOnDisk;
        }
    }
}
