/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetAddress;

import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * This implementation of {@link PersistenceManager} redirects makePersistent
 * requests to the replication master, if it's currently not the local BabuDB.
 * 
 * @author flangner
 * @since 11/04/2010
 */
public class ReplicationPersistenceManager implements PersistenceManager {

    private final ReplicationManager replMan;
    private final PersistenceManager localPersMan;
    
    public ReplicationPersistenceManager(ReplicationManager replMan, 
            PersistenceManager localPersMan) {
        
        this.replMan = replMan;
        this.localPersMan = localPersMan;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#makePersistent(byte, 
     *          org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @Override
    public <T> DatabaseRequestResult<T> makePersistent(byte type, 
            ReusableBuffer load) throws BabuDBException {
        
        if (replMan.isMaster()) {
                        
            return localPersMan.makePersistent(type, load);
        } else {
            InetAddress master = replMan.getMaster();
            // TODO send rq to master
            return null;
        }
    }

/*
 * unsupported in distributed context
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#lockService()
     */
    @Override
    public void lockService() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#unlockService()
     */
    @Override
    public void unlockService() { 
        throw new UnsupportedOperationException();
    }
}
