/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.xtreemfs.babudb.api.InMemoryProcessing;
import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.dev.PersistenceManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.replication.LockableService;
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
class PersistenceManagerProxy extends PersistenceManagerInternal implements LockableService {

    private final ReplicationManager            replMan;
    private final PersistenceManagerInternal    localPersMan;
    private final Policy                        replicationPolicy;
    private final RemoteAccessClient            client;
    private final AtomicInteger                 accessCounter = new AtomicInteger(0);
    private boolean                             locked = false;
    
    public PersistenceManagerProxy(ReplicationManager replMan, 
            PersistenceManagerInternal localPersMan, Policy replicationPolicy, 
            RemoteAccessClient client) {
        
        this.client = client;
        this.replicationPolicy = replicationPolicy;
        this.replMan = replMan;
        this.localPersMan = localPersMan;
        
        // copy in memory processing logic from the local persistence manager
        for (Entry<Byte, InMemoryProcessing> e : localPersMan.getProcessingLogic().entrySet()) {
            registerInMemoryProcessing(e.getKey(), e.getValue());
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#makePersistent(byte, java.lang.Object[], 
     *          org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @Override
    public <T> DatabaseRequestResult<T> makePersistent(byte type, Object[] args, 
            ReusableBuffer serialized) throws BabuDBException {
        
        InetSocketAddress master = getServerToPerformAt(type);
        if (master == null) {
            
            // check if this service has been locked and increment the access counter
            synchronized (accessCounter) {
                if (locked) {
                    throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
                            "This service has currently been locked by the replication plugin.");
                } else {
                    accessCounter.incrementAndGet();
                }   
            }
            return executeLocallyAndReplicate(type, serialized);
        } else {      
            
            return redirectToMaster(type, serialized, master);
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
    private <T> DatabaseRequestResult<T> executeLocallyAndReplicate(byte type, 
            ReusableBuffer payload) throws BabuDBException {
        
        final ListenerWrapper<T> wrapper = new ListenerWrapper<T>();
                
        localPersMan.makePersistent(type, payload).registerListener(
                (DatabaseRequestListener<Object>) new DatabaseRequestListener<Object>() {
        
            @Override
            public void finished(Object result, Object context) {
                
                // request has finished. decrement the access counter
                synchronized (accessCounter) {
                    if (accessCounter.decrementAndGet() == 0) {
                        accessCounter.notify();
                    }
                }
                
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
                
                ReplicateResponse rp = replMan.replicate(le);
                if (!rp.hasFailed()) {
                    replMan.subscribeListener(rp);
                }
            }
            
            @Override
            public void failed(BabuDBException error, Object context) {
                
                // request has finished. decrement the access counter
                synchronized (accessCounter) {
                    if (accessCounter.decrementAndGet() == 0) {
                        accessCounter.notify();
                    }
                }
                
                wrapper.finished(error);
            }
        });
        
        DatabaseRequestResult<T> result = 
            (DatabaseRequestResult<T>) new DatabaseRequestResult<T>() {
        
            @Override
            public void registerListener(
                    DatabaseRequestListener<T> listener) {
                
                wrapper.registerListener(listener);
            }
            
            @Override
            public T get() throws BabuDBException {
                
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
     * @param master
     * @return the request response future.
     */
    <T> DatabaseRequestResult<T> redirectToMaster(byte type, ReusableBuffer load, 
            InetSocketAddress master) {
        
        final ClientResponseFuture<T, ErrorCodeResponse> rp = 
            client.makePersistent(master, type, load);

        DatabaseRequestResult<T> result = new DatabaseRequestResult<T>() {
            
            @Override
            public void registerListener(final  DatabaseRequestListener<T> listener) {
                
                rp.registerListener(new ClientResponseAvailableListener<T>() {
        
                    @Override
                    public void responseAvailable(T r) {
                        listener.finished(r, null);
                    }
    
                    @Override
                    public void requestFailed(Exception e) {
                        listener.failed(new BabuDBException(ErrorCode.IO_ERROR, e.getMessage()), 
                                null);
                    }
                });
            }
            
            @Override
            public T get() throws BabuDBException {
                
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
     * 
     * @return the host to perform the request at, or null, if it is permitted to perform the 
     *         request locally.
     * @throws BabuDBException if replication is currently not available.
     */
    private InetSocketAddress getServerToPerformAt (byte type) throws BabuDBException {
        
        InetSocketAddress master = replMan.getMaster();
         
        if (master == null) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
                    "A majority of servers is currently not available.");
        }
               
        if ((replMan.isItMe(master)) ||
                
            (type == PAYLOAD_TYPE_INSERT && !replicationPolicy.insertIsMasterRestricted()) ||
            
            ((type == PAYLOAD_TYPE_SNAP || type == PAYLOAD_TYPE_SNAP_DELETE) && 
                    !replicationPolicy.snapshotManipultationIsMasterRestricted()) ||
                    
            ((type == PAYLOAD_TYPE_CREATE || type == PAYLOAD_TYPE_COPY || 
                    type == PAYLOAD_TYPE_DELETE) && 
                    !replicationPolicy.dbModificationIsMasterRestricted())) {
            
            return null;
        }
        
        return master;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#lockService()
     */
    @Override
    public void lockService() throws InterruptedException {
        localPersMan.lockService();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#unlockService()
     */
    @Override
    public void unlockService() { 
        localPersMan.unlockService();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#setLogger(org.xtreemfs.babudb.log.DiskLogger)
     */
    @Override
    public void setLogger(DiskLogger logger) {
        localPersMan.setLogger(logger);
    }
    

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#getLatestOnDiskLSN()
     */
    @Override
    public LSN getLatestOnDiskLSN() {
        return localPersMan.getLatestOnDiskLSN();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#init(org.xtreemfs.babudb.lsmdb.LSN)
     */
    @Override
    public void init(LSN initial) {
        localPersMan.init(initial);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.proxy.LockableService#lock()
     */
    @Override
    public void lock() throws InterruptedException {
        synchronized (accessCounter) {
            locked = true;
            while (locked && accessCounter.get() > 0) {
                accessCounter.wait();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.proxy.LockableService#unlock()
     */
    @Override
    public void unlock() {
        synchronized (accessCounter) {
            locked = false;
            accessCounter.notifyAll();
        }
    }

    /**
     * Wrapper class for encapsulating and updating the 
     * {@link DatabaseRequestListener} connected to the request results.
     * 
     * @author flangner
     * @since 19.01.2011
     */
    private final static class ListenerWrapper<T> {
        
        private boolean finished = false;
        private BabuDBException exception = null;
        private DatabaseRequestListener<T> listener = null;
        
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
        
        private synchronized void registerListener(DatabaseRequestListener<T> listener) {
            
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
