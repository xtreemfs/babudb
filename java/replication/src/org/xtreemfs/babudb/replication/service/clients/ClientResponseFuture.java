/*
 * Copyright (c) 2010, Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
 * 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this 
 * list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * Neither the name of the Konrad-Zuse-Zentrum fuer Informationstechnik Berlin 
 * nor the names of its contributors may be used to endorse or promote products 
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.service.clients;

import java.io.IOException;

import org.xtreemfs.babudb.replication.transmission.PBRPCClientAdapter.ErrorCodeException;
import org.xtreemfs.foundation.pbrpc.client.RPCResponse;
import org.xtreemfs.foundation.pbrpc.client.RPCResponseAvailableListener;

/**
 * Proxy for receiving responses on requests done by the replication client.
 * 
 * @param <T> - the result's type.
 * 
 * @author flangner
 * @since 01/04/2011
 */
public abstract class ClientResponseFuture<T> {

    @SuppressWarnings("rawtypes")
    private final RPCResponse original; 
        
    /**
     * The constructor registers Google's original PBRPC response future.
     * 
     * @param rp - the PBRPC response future.
     */
    @SuppressWarnings("rawtypes")
    public ClientResponseFuture(RPCResponse rp) {
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void registerListener (
            final ClientResponseAvailableListener<T> listener) {
        
        if (this.original == null) {
            try {
                listener.responseAvailable(get());
            } catch (Exception e) {
                listener.requestFailed(e);
            }
        }
        
        this.original.registerListener(new RPCResponseAvailableListener() {

            @Override
            public void responseAvailable(RPCResponse rp) {
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
