/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.clients;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.interfaces.LSN;
import org.xtreemfs.babudb.interfaces.LogEntry;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateResponse;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.foundation.oncrpc.client.ONCRPCClient;
import org.xtreemfs.include.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponseDecoder;

/**
 * Client to communicate with the slave. Supports the replication.
 * 
 * @author flangner
 * @since 05/08/2009
 */

public class SlaveClient extends ONCRPCClient {

    public SlaveClient(RPCNIOSocketClient client, InetSocketAddress defaultServer) {
        super(client, defaultServer, 1, ReplicationInterface.getVersion());
    }

    /**
     * The slave is requested to replicate the given LogEntry identified by its
     * {@link org.xtreemfs.babudb.lsmdb.LSN}.
     * 
     * @param lsn
     * @param data
     * @return the {@link RPCResponse}.
     */
    public RPCResponse<?> replicate (org.xtreemfs.babudb.lsmdb.LSN lsn, ReusableBuffer data) {
        replicateRequest rq = new replicateRequest(
                new LSN(lsn.getViewId(),lsn.getSequenceNo()),new LogEntry(data));
        
        RPCResponse<?> r = sendRequest(null, rq.getTag(), rq, new RPCResponseDecoder<Object>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.include.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.include.common.buffer.ReusableBuffer)
             */
            @Override
            public Object getResult(ReusableBuffer data) {
                final replicateResponse rp = new replicateResponse();
                rp.deserialize(data);
                return null;
            }
        });
        
        return r;
    }
}