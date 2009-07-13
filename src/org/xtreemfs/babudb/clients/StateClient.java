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
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.stateRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.stateResponse;
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
        
        RPCResponse<LSN> r = (RPCResponse<LSN>) sendRequest(null, rq.getOperationNumber(), rq, new RPCResponseDecoder<org.xtreemfs.babudb.lsmdb.LSN>() {
        
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
}