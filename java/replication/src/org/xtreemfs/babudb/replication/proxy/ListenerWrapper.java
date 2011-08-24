/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import static org.xtreemfs.babudb.replication.transmission.ErrorCode.mapTransmissionError;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.proxy.BabuDBProxy.RequestRerunner;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.ClientResponseAvailableListener;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter.ErrorCodeException;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Wrapper class for encapsulating and updating the {@link DatabaseRequestListener} connected to the request results.
 * 
 * @author flangner
 * @since 19.01.2011
 */
final class ListenerWrapper<T> implements ClientResponseAvailableListener<T>, SyncListener, 
        DatabaseRequestListener<T> {
    
    interface RequestOperation<T> {
        
        /**
         * Method to execute the request. Has to be repeatable if retry on failure is permitted.
         * @param listener
         */
        void execute(ListenerWrapper<T> listener);
    }
    
    private final BabuDBRequestResultImpl<T> requestFuture;
    private final T                          result;
    private int                              tries = 0;
    private final RequestOperation<T>        request;
    private final RequestRerunner            rerunner;

    public ListenerWrapper(BabuDBRequestResultImpl<T> requestFuture) {
        this(requestFuture, null);
    }
    
    public ListenerWrapper(BabuDBRequestResultImpl<T> requestFuture, RequestOperation<T> request, 
            RequestRerunner rerunner) {
        this(requestFuture, null, request, rerunner);
    }
    
    public ListenerWrapper(BabuDBRequestResultImpl<T> requestFuture, T result) {
        this(requestFuture, result, null, null);
    }
    
    public ListenerWrapper(BabuDBRequestResultImpl<T> requestFuture, T result, RequestOperation<T> request, 
            RequestRerunner rerunner) {
        this.requestFuture = requestFuture;
        this.result = result;
        this.rerunner = rerunner;
        this.request = request;
        if (request != null) tryAgain();
    }
    
    
    void tryAgain() {
        request.execute(this);
    }
    
/*
 * ClientResponseAvailableListener interface
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.
     *              ClientResponseAvailableListener#responseAvailable(java.lang.Object)
     */
    @Override
    public void responseAvailable(T r) {
        requestFuture.finished(r);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.
     *              ClientResponseAvailableListener#requestFailed(java.lang.Exception)
     */
    @Override
    public void requestFailed(Exception e) {
        Logging.logMessage(Logging.LEVEL_INFO, this, "Client request failed, because %s.", e.getMessage());
        Logging.logError(Logging.LEVEL_DEBUG, this, e);
        
        if (e instanceof ErrorCodeException) {
            ErrorCode code = mapTransmissionError(((ErrorCodeException) e).getCode());
            
            if (code == ErrorCode.REPLICATION_FAILURE) {
                checkForRetry(new BabuDBException(code, e.getMessage()));
            } else {
                requestFuture.failed(new BabuDBException(code, e.getMessage()));
            }
        } else {
            
            checkForRetry(new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage()));
        }
    }

/*
 * SyncListner interface
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.log.SyncListener#synced(org.xtreemfs.babudb.log.LogEntry)
     */
    @Override
    public void synced(LSN lsn) {
        requestFuture.finished(result, lsn);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.log.SyncListener#failed(org.xtreemfs.babudb.log.LogEntry, 
     *              java.lang.Exception)
     */
    @Override
    public void failed(Exception e) {
        Logging.logMessage(Logging.LEVEL_INFO, this, "Sync request failed, because %s.", e.getMessage());
        Logging.logError(Logging.LEVEL_DEBUG, this, e);
        
        checkForRetry(new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage()));
    }

/*
 * DatabaseRequestListner interface
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRequestListener#failed(org.xtreemfs.babudb.api.exception.BabuDBException, java.lang.Object)
     */
    @Override
    public void failed(BabuDBException e, Object context) {
        Logging.logMessage(Logging.LEVEL_INFO, this, "Local request failed, because %s.", e.getMessage());
        Logging.logError(Logging.LEVEL_DEBUG, this, e);
        
        requestFuture.failed(e);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRequestListener#finished(java.lang.Object, java.lang.Object)
     */
    @Override
    public void finished(T r, Object context) {
        requestFuture.finished(r);
    }
    
    /**
     * Method to check if it is possible to try the execution again.
     * 
     * @param e
     */
    private final void checkForRetry(BabuDBException e) {
        
        if (request != null && 
           (ReplicationConfig.PROXY_MAX_RETRIES == 0 || tries++ < ReplicationConfig.PROXY_MAX_RETRIES)) {
            
            //handed-over this to RequestRerunner (sleeps for retry delay)
            rerunner.scheduleRequestRerun(this);
        } else {
            requestFuture.failed(e);
        }
    }
}