/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.clients;

import java.io.IOException;

import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter.ErrorCodeException;
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

    private final RPCResponse<M> original; 
        
    /**
     * The constructor registers Google's original PBRPC response future.
     * 
     * @param rp - the PBRPC response future.
     */
    public ClientResponseFuture(RPCResponse<M> rp) {
        this.original = rp;
    }
    
    /**
     * Waits synchronously for the response. May only be accessed once because
     * the requests buffers will be freed afterwards.
     * 
     * @return the generic return value gathered with the request.
     * @throws ErrorCodeException
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract T get() throws ErrorCodeException, IOException, 
                                   InterruptedException;
    
    /**
     * Method to register a response listener to wait asynchronously for the
     * response.
     * 
     * @param listener
     */
    public void registerListener (
            final ClientResponseAvailableListener<T> listener) {
        
        if (this.original == null) {
            try {
                listener.responseAvailable(get());
            } catch (Exception e) {
                listener.requestFailed(e);
            }
        }
        
        this.original.registerListener(new RPCResponseAvailableListener<M>() {

            @Override
            public void responseAvailable(RPCResponse<M> rp) {
                try {
                    listener.responseAvailable(get());
                } catch (Exception e) {
                    listener.requestFailed(e);
                }
            }
        });
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
