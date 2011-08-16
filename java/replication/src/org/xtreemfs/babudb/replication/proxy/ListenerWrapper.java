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
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.ClientResponseAvailableListener;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter.ErrorCodeException;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Wrapper class for encapsulating and updating the 
 * {@link DatabaseRequestListener} connected to the request results.
 * 
 * @author flangner
 * @since 19.01.2011
 */
final class ListenerWrapper<T> implements ClientResponseAvailableListener<T>, SyncListener {
    
    private final BabuDBRequestResultImpl<T> requestFuture;
    private final T                          result;
    
    public ListenerWrapper(BabuDBRequestResultImpl<T> requestFuture) {
        this.requestFuture = requestFuture;
        this.result = null;
    }
    
    public ListenerWrapper(BabuDBRequestResultImpl<T> requestFuture, T result) {
        this.requestFuture = requestFuture;
        this.result = result;
    }
    
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
            requestFuture.failed(new BabuDBException(mapTransmissionError(
                    ((ErrorCodeException) e).getCode()),e.getMessage()));
        } else {
            requestFuture.failed(new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage()));
        }
    }

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
        
        requestFuture.failed(new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage()));
    }
}
