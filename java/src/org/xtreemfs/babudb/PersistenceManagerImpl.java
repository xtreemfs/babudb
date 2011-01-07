/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * Default implementation of the {@link PersistenceManager} interface using
 * the {@link DiskLogger} to let operations become persistent.
 * 
 * @author flangner
 * @since 11/03/2010
 */
class PersistenceManagerImpl implements PersistenceManager {

    private DiskLogger diskLogger;
    
    /**
     * Registers the {@link DiskLogger} at this instance of 
     * {@link PersistenceManager}.
     * 
     * @param logger
     */
    void setLogger(DiskLogger logger) {
        this.diskLogger = logger;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#makePersistent(byte, 
     *          org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @Override
    public <T> DatabaseRequestResult<T> makePersistent(byte type, 
            ReusableBuffer load) throws BabuDBException {
        
        final BabuDBRequestResultImpl<T> result =
            new BabuDBRequestResultImpl<T>();
        
        // build the entry
        LogEntry entry = new LogEntry(load, new SyncListener() {
            
            @Override
            public void synced(LogEntry entry) {
                if (entry != null) entry.free();
                result.finished();
            }
            
            @Override
            public void failed(LogEntry entry, Exception ex) {
                if (entry != null) entry.free();
                result.failed((ex != null && ex instanceof BabuDBException) ? 
                        (BabuDBException) ex : new BabuDBException(
                                ErrorCode.INTERNAL_ERROR, ex.getMessage()));
            }
        }, type);
        
        // append it to the DiskLogger
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
        this.diskLogger.lockLogger();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#unlockService()
     */
    @Override
    public void unlockService() {
        if (this.diskLogger.hasLock()) this.diskLogger.unlockLogger();
    }

}
