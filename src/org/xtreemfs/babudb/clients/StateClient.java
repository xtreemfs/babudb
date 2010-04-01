/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.clients;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.fleaseRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.fleaseResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.localTimeRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.localTimeResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.stateRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.stateResponse;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.oncrpc.client.ONCRPCClient;
import org.xtreemfs.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.foundation.oncrpc.client.RPCResponseDecoder;
import org.xtreemfs.foundation.oncrpc.utils.XDRUnmarshaller;

/**
 * Client to request the state of any client.
 * 
 * @author flangner
 * @since 06/05/2009
 */

public class StateClient extends ONCRPCClient {

    private final InetSocketAddress local;
    
    public StateClient(RPCNIOSocketClient client, InetSocketAddress defaultServer, 
            InetSocketAddress localServer) {
        super(client, defaultServer, 1, ReplicationInterface.getVersion());
        this.local = localServer;
    }

    /**
     * The {@link LSN} of the latest written LogEntry.
     * 
     * @return the {@link RPCResponse} receiving a state as {@link LSN}.
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<LSN> getState () {
        stateRequest rq = new stateRequest();
        
        RPCResponse<LSN> r = (RPCResponse<LSN>) sendRequest(null, rq.getTag(), rq, new RPCResponseDecoder<LSN>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.foundation.buffer.ReusableBuffer)
             */
            @Override
            public LSN getResult(ReusableBuffer data) {
                final stateResponse rp = new stateResponse();
                rp.unmarshal(new XDRUnmarshaller(data));
                
                return new LSN(rp.getReturnValue());
            }
        });
        
        return r;
    }
    
    /**
     * The local time-stamp of the registered participant.
     * 
     * @return the {@link RPCResponse} receiving a time-stamp as {@link Long}.
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<Long> getLocalTime () {
        localTimeRequest rq = new localTimeRequest();
        
        RPCResponse<Long> r = (RPCResponse<Long>) sendRequest(null, rq.getTag(), rq, new RPCResponseDecoder<Long>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.foundation.buffer.ReusableBuffer)
             */
            @Override
            public Long getResult(ReusableBuffer data) {
                final localTimeResponse rp = new localTimeResponse();
                rp.unmarshal(new XDRUnmarshaller(data));
                return rp.getReturnValue();
            }
        });
        
        return r;
    }
    
    /**
     * Sends a {@link FleaseMessage} to the client.
     * 
     * @return the {@link RPCResponse} as proxy for the response.
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<Object> flease (FleaseMessage message) {
        assert (local != null);
        
        ReusableBuffer buffer = BufferPool.allocate(message.getSize());
        message.serialize(buffer);
        buffer.flip();
        
        fleaseRequest rq = new fleaseRequest(buffer, 
                local.getAddress().getHostAddress(), local.getPort());
        
        RPCResponse<Object> r = (RPCResponse<Object>) sendRequest(null, rq.getTag(), rq, new RPCResponseDecoder<Object>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.foundation.buffer.ReusableBuffer)
             */
            @Override
            public Object getResult(ReusableBuffer data) {
                final fleaseResponse rp = new fleaseResponse();
                rp.unmarshal(new XDRUnmarshaller(data));
                return null;
            }
        });
        
        return r;
    }
    
    /**
     * @return the localAddress transmitted with each FLease message send by 
     *         this client.
     */
    public InetSocketAddress getLocalAddress() {
        return local;
    }
}