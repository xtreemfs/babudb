/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission.client;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.clients.ClientInterface;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;
import org.xtreemfs.babudb.interfaces.Chunk;
import org.xtreemfs.babudb.interfaces.DBFileMetaDataSet;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.interfaces.LogEntries;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.chunkRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.chunkResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.fleaseRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.fleaseResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.heartbeatRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.heartbeatResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.loadRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.loadResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.localTimeRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.localTimeResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicaRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicaResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateResponse;
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
 * Abstraction from the underlying ONCRPC Client architecture.
 * 
 * @author flangner
 * @since 06/05/2009
 */

public class Client extends ONCRPCClient implements MasterClient, SlaveClient, 
    ConditionClient {

    /** the address to access on connecting with the local host */
    private final InetSocketAddress local;
    
    /**
     * Default constructor to create a RPCNIOSocketClient abstraction.
     * 
     * @param client
     * @param defaultServer
     * @param localServer
     */
    public Client(RPCNIOSocketClient client, InetSocketAddress defaultServer, 
            InetSocketAddress localServer) {
        super(client, defaultServer, 1, ReplicationInterface.getVersion());
        this.local = localServer;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.client.ClientInterface#getDefaultServerAddress()
     */
    public InetSocketAddress getDefaultServerAddress() {
        return super.getDefaultServerAddress();
    }
    
/*
 * Shared requests
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.client.ConditionClient#state()
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<LSN> state () {
        stateRequest rq = new stateRequest();
        
        RPCResponse<LSN> r = 
            (RPCResponse<LSN>) sendRequest(null, rq.getTag(), rq, 
                    new RPCResponseDecoder<LSN>() {
        
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
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.client.ConditionClient#time()
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<Long> time () {
        localTimeRequest rq = new localTimeRequest();
        
        RPCResponse<Long> r = 
            (RPCResponse<Long>) sendRequest(null, rq.getTag(), rq, 
                    new RPCResponseDecoder<Long>() {
        
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
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.client.ConditionClient#flease(org.xtreemfs.foundation.flease.comm.FleaseMessage)
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<Object> flease (FleaseMessage message) {
        assert (local != null);
        
        ReusableBuffer buffer = BufferPool.allocate(message.getSize());
        message.serialize(buffer);
        buffer.flip();
        
        fleaseRequest rq = new fleaseRequest(buffer, 
                local.getAddress().getHostAddress(), local.getPort());
        
        RPCResponse<Object> r = 
            (RPCResponse<Object>) sendRequest(null, rq.getTag(), rq, 
                    new RPCResponseDecoder<Object>() {
        
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
        },null,true);
        
        return r;
    }
    
/*
 * Master requests
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.client.MasterClient#getReplica(org.xtreemfs.babudb.interfaces.LSNRange)
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<LogEntries> replica (LSNRange range) {
        replicaRequest rq = new replicaRequest(range);
        
        RPCResponse<LogEntries> r = 
            (RPCResponse<LogEntries>) sendRequest(null, rq.getTag(), rq, 
                    new RPCResponseDecoder<LogEntries>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.foundation.buffer.ReusableBuffer)
             */
            @Override
            public LogEntries getResult(ReusableBuffer data) {
                final replicaResponse rp = new replicaResponse();
                rp.unmarshal(new XDRUnmarshaller(data));
                return rp.getReturnValue();
            }
        });
        
        return r;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.client.MasterClient#load(org.xtreemfs.babudb.lsmdb.LSN)
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<DBFileMetaDataSet> load (LSN lsn) {
        loadRequest rq = new loadRequest(
                new org.xtreemfs.babudb.interfaces.LSN(
                        lsn.getViewId(),
                        lsn.getSequenceNo()));
        
        RPCResponse<DBFileMetaDataSet> r = 
            (RPCResponse<DBFileMetaDataSet>) sendRequest(null, rq.getTag(), rq, 
                    new RPCResponseDecoder<DBFileMetaDataSet>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.foundation.buffer.ReusableBuffer)
             */
            @Override
            public DBFileMetaDataSet getResult(ReusableBuffer data) {
                final loadResponse rp = new loadResponse();
                rp.unmarshal(new XDRUnmarshaller(data));
                return rp.getReturnValue();
            }
        });
        
        return r;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.client.MasterClient#chunk(org.xtreemfs.babudb.interfaces.Chunk)
     */
    @SuppressWarnings("unchecked")
    public RPCResponse<ReusableBuffer> chunk (Chunk chunk) {
        chunkRequest rq = new chunkRequest(chunk);
        
        RPCResponse<ReusableBuffer> r = 
            (RPCResponse<ReusableBuffer>) sendRequest(null, rq.getTag(), rq, 
                    new RPCResponseDecoder<ReusableBuffer>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.foundation.buffer.ReusableBuffer)
             */
            @Override
            public ReusableBuffer getResult(ReusableBuffer data) {
                final chunkResponse rp = new chunkResponse();
                rp.unmarshal(new XDRUnmarshaller(data));
                return rp.getReturnValue();
            }
        });
        
        return r;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.client.ConditionClient#heartbeat(org.xtreemfs.babudb.lsmdb.LSN)
     */
    public RPCResponse<?> heartbeat (LSN lsn) {
        heartbeatRequest rq = new heartbeatRequest(
                new org.xtreemfs.babudb.interfaces.LSN(
                        lsn.getViewId(),
                        lsn.getSequenceNo()));

        RPCResponse<?> r = sendRequest(null, rq.getTag(), rq, 
                new RPCResponseDecoder<Object>() {
        
            /*
             * (non-Javadoc)
             * @see org.xtreemfs.foundation.oncrpc.client.RPCResponseDecoder#getResult(org.xtreemfs.foundation.buffer.ReusableBuffer)
             */
            @Override
            public Object getResult(ReusableBuffer data) {
                final heartbeatResponse rp = new heartbeatResponse();
                rp.unmarshal(new XDRUnmarshaller(data));
                return null;
            }
        });
        
        return r;
    }
    
/*
 * Slave requests
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.client.SlaveClient#replicate(org.xtreemfs.babudb.lsmdb.LSN, org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    public RPCResponse<?> replicate (LSN lsn, ReusableBuffer data) {
        replicateRequest rq = new replicateRequest(
                new org.xtreemfs.babudb.interfaces.LSN(
                        lsn.getViewId(),lsn.getSequenceNo()),
                new org.xtreemfs.babudb.interfaces.LogEntry(data));
        
        RPCResponse<?> r = sendRequest(null, rq.getTag(), rq, 
                new RPCResponseDecoder<Object>() {
        
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
    
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ClientInterface) {
            return getDefaultServerAddress().equals(
                    ((ClientInterface) obj).getDefaultServerAddress());
        }
        return false;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "RPC-Client for server " + local.toString() + ".";
    }
}