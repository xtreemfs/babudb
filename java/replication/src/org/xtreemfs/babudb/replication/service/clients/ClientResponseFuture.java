/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.clients;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter.ErrorCodeException;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.pbrpc.client.RPCResponse;
import org.xtreemfs.foundation.pbrpc.client.RPCResponseAvailableListener;

import com.google.protobuf.Message;

/**
 * Proxy for receiving responses on requests done by the replication client.
 * 
 * @param <T> - the result's type.
 * @param <M> - message type of the PRCResponse.
 * 
 * @author flangner
 * @since 01/04/2011
 */
public abstract class ClientResponseFuture<T,M extends Message> {
        
    private ClientResponseAvailableListener<T>  listener = null;

    private final AtomicBoolean                 finished = new AtomicBoolean(false);
    
    private T                                   result = null;
    
    private Exception                           error = null;
    
    
    /**
     * The constructor registers Google's original PBRPC response future.
     * 
     * @param rp - the PBRPC response future.
     */
    public ClientResponseFuture(RPCResponse<M> rp) {
        
        rp.registerListener(new RPCResponseAvailableListener<M>() {
            
            @Override
            public void responseAvailable(RPCResponse<M> rp) {
                
                synchronized (finished) {
                    
                    // request can only be finished once
                    if (finished.compareAndSet(false, true)) {
                        try {
                            result = resolve(rp.get(), rp.getData());
                            if (listener != null) {
                                listener.responseAvailable(result);
                            }
                        } catch (Exception e) {
                            error = e;
                            if (listener != null) {
                                listener.requestFailed(error);
                            }
                        } finally {
                            rp.freeBuffers();
                        }
                    } else {
                        assert (false) : "RPC infrastructure has been corrupted! A request has been answered twice!";
                    }
                    
                    finished.notifyAll();
                }
            }
        });
    }
    
    /**
     * Waits synchronously for the response. 
     * 
     * @return the generic return value gathered with the request.
     * @throws Exception
     */
    public final T get() throws Exception {
        
        synchronized (finished) {
            if (!finished.get()) {
                finished.wait();
            }
            
            if (error != null) {
                throw error;
            } else {
                return result;
            }
        }
    }
    
    /**
     * Method to resolve and interpret the received response. 
     * 
     * @param response
     * @param data
     * 
     * @return the generic return value gathered with the request.
     * @throws ErrorCodeException if message contained an error number.
     * @throws IOException if message could not have been decoded.
     */
    public abstract T resolve(M response, ReusableBuffer data) throws ErrorCodeException, 
            IOException;
    
    /**
     * Method to register a response listener to wait asynchronously for the
     * response.
     * 
     * @param listener
     */
    public final void registerListener (final ClientResponseAvailableListener<T> listener) {
        
        synchronized (finished) {
            if (finished.get()) {
                if (error == null) {
                    listener.responseAvailable(result);
                } else {
                    listener.requestFailed(error);
                }
            } else {
                this.listener = listener;
            }
        }
    }
    
    /**
     * @author flangner
     * @since 01/04/2011
     * @param <U> - the expected result's type.
     */
    public interface ClientResponseAvailableListener<U> {        
        public void responseAvailable(U r);
        
        public void requestFailed(Exception e);
    }
}
