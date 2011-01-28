/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.api.InMemoryProcessing;
import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogger;
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
     *          org.xtreemfs.babudb.api.InMemoryProcessing)
     */
    @Override
    public <T> DatabaseRequestResult<T> makePersistent(byte type, 
            InMemoryProcessing processing) throws BabuDBException {
        
        if (hasPermissionToExecuteLocally(type)) {
            
            return executeLocallyAndReplicate(type, processing);
        } else {      
            
            return redirectToMaster(type, processing.serializeRequest());
        }
    }
    
    /**
     * Executes the request locally and tries to replicate it on the other 
     * participating BabuDB instances.
     * 
     * @param <T>
     * @param type
     * @param load
     * @return the requests result future.
     * @throws BabuDBException
     */
    @SuppressWarnings("unchecked")
    private <T> DatabaseRequestResult<T> executeLocallyAndReplicate(byte type,
                final InMemoryProcessing processing) throws BabuDBException {
        
        final ListenerWrapper wrapper = new ListenerWrapper();
        final AtomicReference<ReusableBuffer> payload = 
            new AtomicReference<ReusableBuffer>(null);
                
        localPersMan.makePersistent(type, new InMemoryProcessing() {
            
            @Override
            public ReusableBuffer serializeRequest() throws BabuDBException {
                payload.set(processing.serializeRequest());
                return payload.get();
            }
            
            @Override
            public void before() throws BabuDBException { processing.before(); }
            
            @Override
            public void after() { processing.after(); }
            
        }).registerListener((DatabaseRequestListener<Object>) 
            new DatabaseRequestListener<Object>() {
        
            @Override
            public void finished(Object result, Object context) {
                
                LogEntry le = (LogEntry) context;
                le.setListener(new SyncListener() {
                
                    @Override
                    public void synced(LogEntry entry) {
                        wrapper.finished(null);
                    }
                    
                    @Override
                    public void failed(LogEntry entry, Exception ex) {
                        wrapper.finished(new BabuDBException(
                                ErrorCode.REPLICATION_FAILURE, 
                                ex.getMessage()));
                    }
                });
                
                ReplicateResponse rp = replMan.replicate(le, payload.get());
                if (!rp.hasFailed()) {
                    replMan.subscribeListener(rp);
                }
            }
            
            @Override
            public void failed(BabuDBException error, Object context) {
                wrapper.finished(error);
            }
        });
        
        DatabaseRequestResult<T> result = (DatabaseRequestResult<T>) 
            new DatabaseRequestResult<Object>() {
        
            @Override
            public void registerListener(
                    DatabaseRequestListener<Object> listener) {
                
                wrapper.registerListener(listener);
            }
            
            @Override
            public Object get() throws BabuDBException {
                
                wrapper.waitForFinish();
                return null;
            }
        };
    
        return result;
    }
    
    /**
     * Executes the request remotely at the BabuDB instance with master 
     * privilege.
     * 
     * @param <T>
     * @param type
     * @param load
     * @return the request response future.
     */
    @SuppressWarnings("unchecked")
    private <T> DatabaseRequestResult<T> redirectToMaster(byte type, 
            ReusableBuffer load) {
        
        final ClientResponseFuture<T> rp = (ClientResponseFuture<T>) 
        client.makePersistent(replMan.getMaster(), type, load);

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
    
    /**
     * @param type - of the request.
     * @return true, if the given type of request may be executed locally. false
     *         otherwise.
     */
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
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#setLogger(org.xtreemfs.babudb.log.DiskLogger)
     */
    @Override
    public void setLogger(DiskLogger logger) {
        localPersMan.setLogger(logger);
    }

    /**
     * Wrapper class for encapsulating and updating the 
     * {@link DatabaseRequestListener} connected to the request results.
     * 
     * @author flangner
     * @since 19.01.2011
     */
    private final static class ListenerWrapper {
        
        private boolean finished = false;
        private BabuDBException exception = null;
        private DatabaseRequestListener<Object> listener = null;
        
        private synchronized void finished(BabuDBException e) {
            
            if (!finished) {
                finished = true;
                exception = e;
                notifyListener();
                notifyAll();
            }
        }
        
        private synchronized void waitForFinish() throws BabuDBException {
            
            try {
                while (!finished) {
                    wait();
                }
            } catch (InterruptedException e) {
                throw new BabuDBException(
                        ErrorCode.INTERRUPTED, e.getMessage());
            }
            
            if (exception != null) {
                throw exception;
            } 
        }
        
        private synchronized void registerListener(
                DatabaseRequestListener<Object> listener) {
            
            this.listener = listener;
            
            if (finished) {
                notifyListener();
            }
        }
        
        private void notifyListener() {
            if (listener != null) {
                if (exception != null) {
                    listener.failed(exception, null);
                } else {
                    listener.finished(null, null);
                }
            }
        }
    }
}
