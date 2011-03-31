/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.mock;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.dev.PersistenceManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * @author flangner
 * @since 02/21/2011
 */

public class PersistenceManagerMock extends PersistenceManagerInternal {

    final AtomicReference<LSN> onDisk = new AtomicReference<LSN>(new LSN(1, 0L));
    
    private final AtomicBoolean lock = new AtomicBoolean();
    private final String name;

    public PersistenceManagerMock(String name) {
        this.name = name;
    }

    @Override
    public LSN getLatestOnDiskLSN() {
        return onDisk.get();
    }

    @Override
    public void init(LSN initial) {
        onDisk.set(initial);
    }

    @Override
    public void lockService() throws InterruptedException {

        lock.set(true);
        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "PersMan of mock '%s' has been locked.", name);
    }

    @Override
	public <T> DatabaseRequestResult<T> makePersistent(byte type,
			Object[] args, ReusableBuffer serialized) throws BabuDBException {
		
		if (lock.get()) {
			throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, "Serivce has been locked!");
		} else {
			synchronized (onDisk) {
				LSN lsn = onDisk.get();
				lsn = new LSN(lsn.getViewId(), lsn.getSequenceNo() + 1L);	
				onDisk.set(lsn);
				Logging.logMessage(Logging.LEVEL_ERROR, this, "PersMan of mock '%s' has retrieved a new operation to perform (%d) with LSN %s.", name, (int) type, lsn.toString());
			}
		}
		return null;
	}

    @Override
    public void setLogger(DiskLogger logger) {
        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "PersMan of mock '%s' registers a new logger (%s).", name,
                logger);
    }

    @Override
    public void unlockService() {
        lock.set(false);
        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "PersMan of mock '%s' has been unlocked.", name);
    }

}
