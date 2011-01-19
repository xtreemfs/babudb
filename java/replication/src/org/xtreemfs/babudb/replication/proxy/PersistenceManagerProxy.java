/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.replication.RemoteAccessClient;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.policy.Policy;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.ClientResponseAvailableListener;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

import static org.xtreemfs.babudb.log.LogEntry.*;

/**
 * This implementation of {@link PersistenceManager} redirects makePersistent
 * requests to the replication master, if it's currently not the local BabuDB.
 * 
 * @author flangner
 * @since 11/04/2010
 */
class PersistenceManagerProxy implements PersistenceManager {

    private final ReplicationManager replMan;
    private final PersistenceManager localPersMan;
    private final Policy             replicationPolicy;
    private final RemoteAccessClient client;
    
    public PersistenceManagerProxy(ReplicationManager replMan, 
            PersistenceManager localPersMan, Policy replicationPolicy, 
            RemoteAccessClient client) {
        
        this.client = client;
        this.replicationPolicy = replicationPolicy;
        this.replMan = replMan;
        this.localPersMan = localPersMan;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#makePersistent(byte, 
     *          org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> DatabaseRequestResult<T> makePersistent(byte type, 
            final ReusableBuffer load) throws BabuDBException {
        
        if (hasPermissionToExecuteLocally(type)) {
            
            final DatabaseRequestResult<T> result = (DatabaseRequestResult<T>) 
                    new DatabaseRequestResult<Object>() {
                
                @Override
                public void registerListener(DatabaseRequestListener<Object> listener) {
                    // TODO Auto-generated method stub
                    
                }
                
                @Override
                public Object get() throws BabuDBException {
                    // TODO Auto-generated method stub
                    return null;
                }
            };
                
            localPersMan.makePersistent(type, load).registerListener(
                    (DatabaseRequestListener<Object>) 
                    new DatabaseRequestListener<Object>() {
                
                @Override
                public void finished(Object result, Object context) {
                    LogEntry le = (LogEntry) context;
                    le.setListener(new SyncListener() {
                        
                        @Override
                        public void synced(LogEntry entry) {
                            // TODO Auto-generated method stub
                            
                        }
                        
                        @Override
                        public void failed(LogEntry entry, Exception ex) {
                            // TODO Auto-generated method stub
                            
                        }
                    });
                    
                    ReplicateResponse rp = replMan.replicate(le, load);
                    if (!rp.hasFailed()) {
                        replMan.subscribeListener(rp);
                    }
                }
                
                @Override
                public void failed(BabuDBException error, Object context) {
                    // TODO result.failed
                }
            });
            
            return result; // TODO return proxy for the real DBRListener
        } else {      
            
            InetSocketAddress master = null;// TODO replMan.getMaster();
            final ClientResponseFuture<T> rp = (ClientResponseFuture<T>) 
                    client.makePersistent(master, type, load);
            
            DatabaseRequestResult<T> result = (DatabaseRequestResult<T>) 
                    new DatabaseRequestResult<Object>() {
                
                @Override
                public void registerListener(final 
                        DatabaseRequestListener<Object> listener) {
                    
                    rp.registerListener((ClientResponseAvailableListener<T>) 
                            new ClientResponseAvailableListener<Object>() {

                                @Override
                                public void responseAvailable(Object r) {
                                    listener.finished(r, null);
                                }

                                @Override
                                public void requestFailed(Exception e) {
                                    listener.failed(new BabuDBException(
                                            ErrorCode.IO_ERROR, e.getMessage()), 
                                            null);
                                }
                    });
                }
                
                @Override
                public Object get() throws BabuDBException {
                    try {
                        return rp.get();
                    } catch (Exception e) {
                        throw new BabuDBException(ErrorCode.IO_ERROR, 
                                e.getMessage());
                    }
                }
            };
            
            return result;
        }
    }
    
    private boolean hasPermissionToExecuteLocally (byte type) {
        
        boolean result = false;
        
        result |= replMan.isMaster();
        
        result |= (type == PAYLOAD_TYPE_INSERT &&
                  !replicationPolicy.insertIsMasterRestricted());
            
        result |= ((type == PAYLOAD_TYPE_SNAP || 
                    type == PAYLOAD_TYPE_SNAP_DELETE) && 
                  !replicationPolicy.snapshotManipultationIsMasterRestricted());
        
        result |= ((type == PAYLOAD_TYPE_CREATE || type == PAYLOAD_TYPE_COPY || 
                    type == PAYLOAD_TYPE_DELETE) && 
                   !replicationPolicy.dbModificationIsMasterRestricted());
        
        return result;
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
