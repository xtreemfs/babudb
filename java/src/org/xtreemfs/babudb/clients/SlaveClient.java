/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.clients;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.interfaces.LSN;
import org.xtreemfs.babudb.interfaces.LogEntry;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateResponse;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.foundation.oncrpc.client.RPCResponseDecoder;
import org.xtreemfs.foundation.oncrpc.utils.XDRUnmarshaller;

/**
 * Client to communicate with the slave. Supports the replication.
 * 
 * @author flangner
 * @since 05/08/2009
 */

public class SlaveClient extends StateClient {

    public SlaveClient(RPCNIOSocketClient client, InetSocketAddress defaultServer,
            InetSocketAddress localAddress) {
        super(client, defaultServer, localAddress);
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
             * @see org.xtreemfs.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.foundation.buffer.ReusableBuffer)
             */
            @Override
            public Object getResult(ReusableBuffer data) {
                final replicateResponse rp = new replicateResponse();
                rp.unmarshal(new XDRUnmarshaller(data));
                return null;
            }
        });
        
        return r;
    }
}