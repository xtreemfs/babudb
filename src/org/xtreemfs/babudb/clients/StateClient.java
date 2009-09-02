/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.clients;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.interfaces.InetAddress;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.remoteStopRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.remoteStopResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.stateRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.stateResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.toMasterRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.toMasterResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.toSlaveRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.toSlaveResponse;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.foundation.oncrpc.client.ONCRPCClient;
import org.xtreemfs.include.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponseDecoder;

/**
 * Client to request the state of any client.
 * 
 * @author flangner
 * @since 06/05/2009
 */

public class StateClient extends ONCRPCClient {

    public StateClient(RPCNIOSocketClient client, InetSocketAddress defaultServer) {
        super(client, defaultServer, 1, ReplicationInterface.getVersion());
    }

    /**
     * The {@link LSN} of the latest written LogEntry.
     * 
     * @return the {@link RPCResponse} receiving a state as {@link LSN}.
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<LSN> getState () {
        stateRequest rq = new stateRequest();
        
        RPCResponse<LSN> r = (RPCResponse<LSN>) sendRequest(null, rq.getTag(), rq, new RPCResponseDecoder<org.xtreemfs.babudb.lsmdb.LSN>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.include.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.include.common.buffer.ReusableBuffer)
             */
            @Override
            public LSN getResult(ReusableBuffer data) {
                final stateResponse rp = new stateResponse();
                rp.deserialize(data);
                org.xtreemfs.babudb.interfaces.LSN result = rp.getReturnValue();
                return new LSN(result.getViewId(),result.getSequenceNo());
            }
        });
        
        return r;
    }
    
    /**
     * <p>
     * Stops the BabuDB of the given client and returns the {@link LSN} 
     * of its latest written LogEntry.
     * </p>
     * 
     * @return the {@link RPCResponse} receiving a state as {@link LSN}.
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<LSN> remoteStop () {
        remoteStopRequest rq = new remoteStopRequest();
        
        RPCResponse<LSN> r = (RPCResponse<LSN>) sendRequest(null, rq.getTag(), rq, new RPCResponseDecoder<org.xtreemfs.babudb.lsmdb.LSN>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.include.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.include.common.buffer.ReusableBuffer)
             */
            @Override
            public LSN getResult(ReusableBuffer data) {
                final remoteStopResponse rp = new remoteStopResponse();
                rp.deserialize(data);
                org.xtreemfs.babudb.interfaces.LSN result = rp.getReturnValue();
                return new LSN(result.getViewId(),result.getSequenceNo());
            }
        });
        
        return r;
    }
    
    /**
     * <p>
     * Switches the mode of the client BabuDB to slave-mode.
     * The BabuDB has to be stopped before!
     * </p>
     * 
     * @param address - of the new master.
     * @return the response proxy.
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<Object> toSlave (InetSocketAddress address) {
        toSlaveRequest rq = new toSlaveRequest(new InetAddress(address.getHostName(),address.getPort()));
        
        RPCResponse<Object> r = (RPCResponse<Object>) sendRequest(null, rq.getTag(), rq, new RPCResponseDecoder<Object>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.include.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.include.common.buffer.ReusableBuffer)
             */
            @Override
            public Object getResult(ReusableBuffer data) {
                final toSlaveResponse rp = new toSlaveResponse();
                rp.deserialize(data);
                return null;
            }
        });
        
        return r;
    }
    
    /**
     * <p>
     * Switches the mode of the client BabuDB to pseudo-master-mode.
     * A checkpoint will be established. The BabuDB has to be stopped before!
     * </p>
     * 
     * @param address - of the new slave.
     * @return the response proxy.
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<Object> toMaster (InetSocketAddress address) {
        toMasterRequest rq = new toMasterRequest(new InetAddress(address.getHostName(),address.getPort()));
        
        RPCResponse<Object> r = (RPCResponse<Object>) sendRequest(null, rq.getTag(), rq, new RPCResponseDecoder<Object>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.include.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.include.common.buffer.ReusableBuffer)
             */
            @Override
            public Object getResult(ReusableBuffer data) {
                final toMasterResponse rp = new toMasterResponse();
                rp.deserialize(data);
                return null;
            }
        });
        
        return r;
    }
}