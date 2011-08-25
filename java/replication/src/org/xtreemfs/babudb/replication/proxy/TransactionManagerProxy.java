/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map.Entry;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.dev.transaction.InMemoryProcessing;
import org.xtreemfs.babudb.api.dev.transaction.TransactionInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.transaction.TransactionListener;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.policy.Policy;
import org.xtreemfs.babudb.replication.proxy.ListenerWrapper.RequestOperation;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

import static org.xtreemfs.babudb.log.LogEntry.*;
import static org.xtreemfs.babudb.api.transaction.Operation.*;
import static org.xtreemfs.babudb.api.dev.transaction.TransactionInternal.containsOperationType;
import static org.xtreemfs.babudb.api.dev.transaction.TransactionInternal.deserialize;

/**
 * This implementation of {@link TransactionManager} redirects makePersistent
 * requests to the replication master, if it's currently not the local BabuDB.
 * 
 * @author flangner
 * @since 11/04/2010
 */
public class TransactionManagerProxy extends TransactionManagerInternal {

    private final ReplicationManager            replMan;
    private final TransactionManagerInternal    localTxnMan;
    private final Policy                        replicationPolicy;
    private final BabuDBProxy                   babuDBProxy;
    
    public TransactionManagerProxy(ReplicationManager replMan, TransactionManagerInternal localTxnMan, 
            Policy replicationPolicy, BabuDBProxy babuDBProxy) {
        
        assert (!(localTxnMan instanceof TransactionManagerProxy));
        
        this.babuDBProxy = babuDBProxy;
        this.replicationPolicy = replicationPolicy;
        this.replMan = replMan;
        this.localTxnMan = localTxnMan;
        
        // copy in memory processing logic from the local persistence manager
        for (Entry<Byte, InMemoryProcessing> e : localTxnMan.getProcessingLogic().entrySet()) {
            registerInMemoryProcessing(e.getKey(), e.getValue());
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#makePersistent(
     *          org.xtreemfs.babudb.api.dev.transaction.TransactionInternal, 
     *          org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @Override
    public void makePersistent(TransactionInternal txn, ReusableBuffer serialized, 
            BabuDBRequestResultImpl<Object> future) throws BabuDBException {
        
        assert (serialized != null);
        
        try {
            InetSocketAddress master = getServerToPerformAt(txn.aggregateOperationTypes(), 0);
            if (master == null) {
                executeLocallyAndReplicate(txn, serialized, future);
            } else {      
                redirectToMaster(serialized, master, future);
            }
        } catch (BabuDBException be) {
            BufferPool.free(serialized);
            throw be;
        }
    }
    
    // TODO ugly code! redesign!!
    public void makePersistentNonBlocking(ReusableBuffer serialized, BabuDBRequestResultImpl<Object> future) 
            throws BabuDBException {
        assert (serialized != null);
        
        try {
            // dezerialize the buffer
            TransactionInternal txn = deserialize(serialized);
            serialized.flip();
            
            InetSocketAddress master = getServerToPerformAt(txn.aggregateOperationTypes(), -1);
            if (master == null) {
                executeLocallyAndReplicate(txn, serialized, future);
            } else {      
                redirectToMaster(serialized, master, future);
            }

        } catch (BabuDBException be) {
            BufferPool.free(serialized);
            throw be;
        }catch (IOException e) {
            if (serialized != null) BufferPool.free(serialized);
            throw new BabuDBException(ErrorCode.IO_ERROR, e.getMessage(), e);
        }
    }
    
    /**
     * Executes the request locally and tries to replicate it on the other 
     * participating BabuDB instances.
     * 
     * @param <T>
     * @param txn
     * @param payload
     * @param future
     * 
     * @return the requests result future.
     * @throws BabuDBException
     */
    private void executeLocallyAndReplicate(TransactionInternal txn, final ReusableBuffer payload, 
            final BabuDBRequestResultImpl<Object> future) throws BabuDBException {
        
        final BabuDBRequestResultImpl<Object> localFuture = 
            new BabuDBRequestResultImpl<Object>(babuDBProxy.getResponseManager());
        localTxnMan.makePersistent(txn, payload.createViewBuffer(), localFuture);
        localFuture.registerListener(new DatabaseRequestListener<Object>() {
        
            @Override
            public void finished(Object result, Object context) {
                                
                LSN assignedByDiskLogger = localFuture.getAssignedLSN();
                LogEntry le = new LogEntry(payload, new ListenerWrapper<Object>(future, result), 
                        PAYLOAD_TYPE_TRANSACTION);
                le.assignId(assignedByDiskLogger.getViewId(), assignedByDiskLogger.getSequenceNo());
                
                ReplicateResponse rp = replMan.replicate(le);
                if (!rp.hasFailed()) {
                    replMan.subscribeListener(rp);
                }
                
                le.free();
            }
            
            @Override
            public void failed(BabuDBException error, Object context) {
                future.failed(error);
            }
        });
    }
    
    /**
     * Executes the request remotely at the BabuDB instance with master 
     * privilege.
     * 
     * @param <T>
     * @param load
     * @param master
     * @param future
     * @return the request response future.
     */
    private void redirectToMaster(final ReusableBuffer load, final InetSocketAddress master, 
            BabuDBRequestResultImpl<Object> future) {
        
        new ListenerWrapper<Object>(future, new RequestOperation<Object>() {
            
            /* (non-Javadoc)
             * @see org.xtreemfs.babudb.replication.proxy.ListenerWrapper.RequestOperation#
             *  execute(org.xtreemfs.babudb.replication.proxy.ListenerWrapper)
             */
            @Override
            public void execute(ListenerWrapper<Object> listener) {
                babuDBProxy.getClient().makePersistent(master, load.createViewBuffer()).registerListener(listener);
            }
        }, babuDBProxy.getRequestRerunner(), load);
    }
        
    /**
     * @param aggregatedType - of the request.
     * @param timeout - 0 means infinitly and < 0 non blocking.
     * 
     * @return the host to perform the request at, or null, if it is permitted to perform the 
     *         request locally.
     * @throws BabuDBException if replication is currently not available.
     */
    private InetSocketAddress getServerToPerformAt (byte aggregatedType, int timeout) throws BabuDBException {
        
        InetSocketAddress master;
        try {
            master = replMan.getMaster(timeout);
        } catch (InterruptedException e) {
            throw new BabuDBException(ErrorCode.INTERRUPTED, 
                    "Waiting for a lease holder was interrupted.", e);
        }
               
        if ((replMan.isItMe(master)) ||
                
            (containsOperationType(aggregatedType, TYPE_GROUP_INSERT) && 
                    !replicationPolicy.insertIsMasterRestricted()) ||
            
            ((containsOperationType(aggregatedType, TYPE_CREATE_SNAP) ||
              containsOperationType(aggregatedType, TYPE_DELETE_SNAP)) && 
                    !replicationPolicy.snapshotManipultationIsMasterRestricted()) ||
                    
            ((containsOperationType(aggregatedType, TYPE_CREATE_DB) ||
              containsOperationType(aggregatedType, TYPE_COPY_DB) ||
              containsOperationType(aggregatedType, TYPE_DELETE_DB)) && 
                    !replicationPolicy.dbModificationIsMasterRestricted())) {
            
            return null;
        }
        
        if (replMan.redirectIsVisible()) {
            throw new BabuDBException(ErrorCode.REDIRECT, master.toString());
        }
        return master;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#lockService()
     */
    @Override
    public void lockService() throws InterruptedException {
        localTxnMan.lockService();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#unlockService()
     */
    @Override
    public void unlockService() { 
        localTxnMan.unlockService();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#setLogger(org.xtreemfs.babudb.log.DiskLogger)
     */
    @Override
    public void setLogger(DiskLogger logger) {
        localTxnMan.setLogger(logger);
    }
    

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#getLatestOnDiskLSN()
     */
    @Override
    public LSN getLatestOnDiskLSN() {
        return localTxnMan.getLatestOnDiskLSN();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.PersistenceManager#init(org.xtreemfs.babudb.lsmdb.LSN)
     */
    @Override
    public void init(LSN initial) {
        localTxnMan.init(initial);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#replayTransaction(org.xtreemfs.babudb.api.dev.transaction.TransactionInternal)
     */
    @Override
    public void replayTransaction(TransactionInternal txn) throws BabuDBException {
        this.localTxnMan.replayTransaction(txn);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#addTransactionListener(org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void addTransactionListener(TransactionListener listener) {
        this.localTxnMan.addTransactionListener(listener);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#removeTransactionListener(org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void removeTransactionListener(TransactionListener listener) {
        this.localTxnMan.removeTransactionListener(listener);
    }
}
