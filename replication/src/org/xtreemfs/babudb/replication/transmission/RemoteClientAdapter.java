/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.transmission;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceClient;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.replication.RemoteAccessClient;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture;
import org.xtreemfs.babudb.replication.transmission.PBRPCClientAdapter.ErrorCodeException;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;
import org.xtreemfs.foundation.pbrpc.client.RPCResponse;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.Auth;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.UserCredentials;

/**
 * RPCClient for delegating BabuDB requests to the instance with master 
 * privilege.
 * 
 * @author flangner
 * @since 01/19/2011
 */
public class RemoteClientAdapter extends RemoteAccessServiceClient 
    implements RemoteAccessClient {

    public RemoteClientAdapter(RPCNIOSocketClient client) {
        super(client, null);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RemoteAccessClient#makePersistent(
     *  java.net.InetSocketAddress, int, 
     *  org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @Override
    public ClientResponseFuture<?> makePersistent(InetSocketAddress master, 
            int type, ReusableBuffer data) {
        assert (master != null);
        
        try {
            final RPCResponse<ErrorCodeResponse> result = makePersistent(master, 
                    Auth.getDefaultInstance(), 
                    UserCredentials.getDefaultInstance(), type, data);
            
            return new ClientResponseFuture<Object>(result) {
                
                @Override
                public Object get() throws IOException, InterruptedException, 
                        ErrorCodeException {
                    try {
                        ErrorCodeResponse response = result.get();
                        if (response.getErrorCode() != 0) {
                            throw new ErrorCodeException(
                                    response.getErrorCode());
                        }
                        return null;
                    } finally {
                        result.freeBuffers();
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<Object>(null) {
                
                @Override
                public Object get() throws IOException {
                    throw e;
                }
            };
        } finally {
            BufferPool.free(data);
        }
    }
}
