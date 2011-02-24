/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.xtreemfs.babudb.lsmdb.LSMDatabase.DBFileMetaData;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.DBFileMetaDatas;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.LogEntries;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Timestamp;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceClient;
import org.xtreemfs.babudb.replication.service.clients.ClientInterface;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;
import org.xtreemfs.babudb.replication.service.logic.LoadLogic.DBFileMetaDataSet;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;
import org.xtreemfs.foundation.pbrpc.client.RPCResponse;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.Auth;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.UserCredentials;

/**
 * Adapter to translate BabuDB specific calls into Google's PBRPC compatible
 * client-calls. 
 * 
 * @author flangner
 * @since 01/04/2011
 */
public class PBRPCClientAdapter extends ReplicationServiceClient 
    implements MasterClient, SlaveClient {

    private final InetSocketAddress defaultServer;
    
    /**
     * @param client
     * @param defaultServer
     */
    public PBRPCClientAdapter(RPCNIOSocketClient client, 
                              InetSocketAddress defaultServer) {
        
        super(client, defaultServer);
        this.defaultServer = defaultServer;
    }

/*
 * ClientInterface:    
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.ClientInterface#
     * getDefaultServerAddress()
     */
    @Override
    public InetSocketAddress getDefaultServerAddress() {
        return defaultServer;
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
        return "RPC-Client for server " + defaultServer.toString() + ".";
    }
/*
 * ConditionClient:
 */

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.ConditionClient#
     * state()
     */
    @Override
    public ClientResponseFuture<org.xtreemfs.babudb.lsmdb.LSN, LSN> state() {
        try {
            final RPCResponse<LSN> result = state(null, 
                    Auth.getDefaultInstance(), 
                    UserCredentials.getDefaultInstance());
        
            return new ClientResponseFuture<org.xtreemfs.babudb.lsmdb.LSN, LSN>(
                    result) {
                
                @Override
                public org.xtreemfs.babudb.lsmdb.LSN get() throws IOException, 
                        InterruptedException {
                    try {
                        LSN response = result.get();
                        
                        return new org.xtreemfs.babudb.lsmdb.LSN(
                                response.getViewId(), response.getSequenceNo());
                    } finally {
                        result.freeBuffers();
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<org.xtreemfs.babudb.lsmdb.LSN,LSN>(
                    null) {
                
                @Override
                public org.xtreemfs.babudb.lsmdb.LSN get() throws IOException {
                    throw e;
                }
            };
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.ConditionClient#
     * state()
     */
    @Override
    public ClientResponseFuture<org.xtreemfs.babudb.lsmdb.LSN, LSN> volatileState() {
        try {
            final RPCResponse<LSN> result = volatileState(null, 
                    Auth.getDefaultInstance(), 
                    UserCredentials.getDefaultInstance());
        
            return new ClientResponseFuture<org.xtreemfs.babudb.lsmdb.LSN,LSN>(
                    result) {
                
                @Override
                public org.xtreemfs.babudb.lsmdb.LSN get() throws IOException, 
                        InterruptedException {
                    try {
                        LSN response = result.get();
                        
                        return new org.xtreemfs.babudb.lsmdb.LSN(
                                response.getViewId(), response.getSequenceNo());
                    } finally {
                        result.freeBuffers();
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<org.xtreemfs.babudb.lsmdb.LSN, LSN>(
                    null) {
                
                @Override
                public org.xtreemfs.babudb.lsmdb.LSN get() throws IOException {
                    throw e;
                }
            };
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.ConditionClient#
     * time()
     */
    @Override
    public ClientResponseFuture<Long,Timestamp> time() {
        try {
            final RPCResponse<Timestamp> result = localTime(null, 
                        Auth.getDefaultInstance(), 
                        UserCredentials.getDefaultInstance());
            
            return new ClientResponseFuture<Long, Timestamp>(result) {
                
                @Override
                public Long get() throws IOException, InterruptedException, 
                        ErrorCodeException {
                    try {
                        Timestamp response = result.get();
                        if (response.getErrorCode() != 0) {
                            throw new ErrorCodeException(
                                    response.getErrorCode());
                        }
                        return response.getValue();
                    } finally {
                        result.freeBuffers();
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<Long,Timestamp>(null) {
                
                @Override
                public Long get() throws IOException {
                    throw e;
                }
            };
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.ConditionClient#flease(
     *          org.xtreemfs.foundation.flease.comm.FleaseMessage)
     */
    @Override
    public ClientResponseFuture<Object, ErrorCodeResponse> flease(FleaseMessage message) {
        ReusableBuffer payload = BufferPool.allocate(message.getSize());
        try {
            InetSocketAddress sender = message.getSender();
            message.serialize(payload);
            final RPCResponse<ErrorCodeResponse> result = flease(null, 
                        Auth.getDefaultInstance(), 
                        UserCredentials.getDefaultInstance(), 
                        sender.getHostName(), sender.getPort(), 
                        payload.createViewBuffer());
            
            return new ClientResponseFuture<Object,ErrorCodeResponse>(result) {
                
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
            return new ClientResponseFuture<Object, ErrorCodeResponse>(null) {
                
                @Override
                public Object get() throws IOException {
                    throw e;
                }
            };
        } finally {
            BufferPool.free(payload);
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.ConditionClient#heartbeat(
     *          org.xtreemfs.babudb.lsmdb.LSN, int)
     */
    @Override
    public ClientResponseFuture<Object, ErrorCodeResponse> heartbeat(org.xtreemfs.babudb.lsmdb.LSN lsn, int port) {
        
        try {
            final RPCResponse<ErrorCodeResponse> result = heartbeat(null, 
                        Auth.getDefaultInstance(), 
                        UserCredentials.getDefaultInstance(), port, 
                        LSN.newBuilder().setViewId(lsn.getViewId())
                                        .setSequenceNo(lsn.getSequenceNo()).build());
            
            return new ClientResponseFuture<Object, ErrorCodeResponse>(result) {
                
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
            return new ClientResponseFuture<Object, ErrorCodeResponse>(null) {
                
                @Override
                public Object get() throws IOException {
                    throw e;
                }
            };
        }
    }

/*
 * MasterClient:    
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.MasterClient#
     * replica(org.xtreemfs.babudb.lsmdb.LSN, org.xtreemfs.babudb.lsmdb.LSN)
     */
    @Override
    public ClientResponseFuture<ReusableBuffer[], LogEntries> replica(
            org.xtreemfs.babudb.lsmdb.LSN start, 
            org.xtreemfs.babudb.lsmdb.LSN end) {
        try {
            LSN s = LSN.newBuilder().setViewId(start.getViewId())
                                    .setSequenceNo(start.getSequenceNo())
                                    .build();
            LSN e = LSN.newBuilder().setViewId(end.getViewId())
                                    .setSequenceNo(end.getSequenceNo())
                                    .build();
            final RPCResponse<LogEntries> result = replica(null, 
                        Auth.getDefaultInstance(), 
                        UserCredentials.getDefaultInstance(), s, e);
                        
            return new ClientResponseFuture<ReusableBuffer[], LogEntries>(result) {
                
                @Override
                public ReusableBuffer[] get() throws IOException, 
                        InterruptedException, ErrorCodeException {
                    try {
                        LogEntries response = result.get();                        
                        if (response.getErrorCode() != 0) {
                            throw new ErrorCodeException(
                                    response.getErrorCode());
                        }
                        int leCount = response.getLogEntriesCount();
                        ReusableBuffer[] r = new ReusableBuffer[leCount];
                        
                        int pos = 0;
                        for (int i = 0; i < leCount; i++) {
                            r[i] = result.getData().createViewBuffer();
                            r[i].position(pos);
                            pos += response.getLogEntries(i).getLength();
                            r[i].limit(pos);
                        }
                        return r;
                    } finally {
                        result.freeBuffers();
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<ReusableBuffer[], LogEntries>(null) {
                
                @Override
                public ReusableBuffer[] get() throws IOException {
                    throw e;
                }
            };
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.MasterClient#
     * chunk(java.lang.String, long, long)
     */
    @Override
    public ClientResponseFuture<ReusableBuffer, ErrorCodeResponse> chunk(String fileName, 
            long start, long end) {
        try {
            final RPCResponse<ErrorCodeResponse> result = chunk(null, 
                        Auth.getDefaultInstance(), 
                        UserCredentials.getDefaultInstance(), fileName, start,
                        end);
            
            return new ClientResponseFuture<ReusableBuffer, ErrorCodeResponse>(result) {
                
                @Override
                public ReusableBuffer get() throws IOException, 
                        InterruptedException, ErrorCodeException {
                    try {
                        ErrorCodeResponse response = result.get();
                        if (response.getErrorCode() != 0) {
                            throw new ErrorCodeException(
                                    response.getErrorCode());
                        }
                        return result.getData().createViewBuffer();
                    } finally {
                        result.freeBuffers();
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<ReusableBuffer, ErrorCodeResponse>(null) {
                
                @Override
                public ReusableBuffer get() throws IOException {
                    throw e;
                }
            };
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.MasterClient#
     * load(org.xtreemfs.babudb.lsmdb.LSN)
     */
    @Override
    public ClientResponseFuture<DBFileMetaDataSet, DBFileMetaDatas> load(
            org.xtreemfs.babudb.lsmdb.LSN lsn) {
        try {
            final RPCResponse<DBFileMetaDatas> result = load(null, 
                        Auth.getDefaultInstance(), 
                        UserCredentials.getDefaultInstance(), lsn.getViewId(), 
                        lsn.getSequenceNo());
            
            return new ClientResponseFuture<DBFileMetaDataSet, DBFileMetaDatas>(result) {
                
                @Override
                public DBFileMetaDataSet get() throws IOException, 
                        InterruptedException, ErrorCodeException {
                    try {
                        DBFileMetaDatas rp = result.get();
                        if (rp.getErrorCode() != 0) {
                            throw new ErrorCodeException(
                                    rp.getErrorCode());
                        }
                        List<DBFileMetaData> c = 
                            new ArrayList<DBFileMetaData>();
                        
                        for (int i = 0; i < rp.getDbFileMetadatasCount(); i++) {
                            org.xtreemfs.babudb.pbrpc.GlobalTypes.DBFileMetaData 
                                md = rp.getDbFileMetadatas(i);
                            
                            c.add(new DBFileMetaData(md.getFileName(), 
                                                     md.getFileSize()));
                        }
                        
                        return new DBFileMetaDataSet(rp.getMaxChunkSize(), c);
                    } finally {
                        result.freeBuffers();
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<DBFileMetaDataSet, DBFileMetaDatas>(null) {
                
                @Override
                public DBFileMetaDataSet get() throws IOException {
                    throw e;
                }
            };
        }
    }
    
/*
 * SlaveClient:
 */

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.clients.SlaveClient#
     * replicate(org.xtreemfs.babudb.lsmdb.LSN, 
     *           org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @Override
    public ClientResponseFuture<Object, ErrorCodeResponse> replicate(org.xtreemfs.babudb.lsmdb.LSN lsn, 
            ReusableBuffer data) {
        try {
            final RPCResponse<ErrorCodeResponse> result = replicate(null, 
                        Auth.getDefaultInstance(), 
                        UserCredentials.getDefaultInstance(), lsn.getViewId(), 
                        lsn.getSequenceNo(), data);
            
            return new ClientResponseFuture<Object, ErrorCodeResponse>(result) {
                
                @Override
                public ReusableBuffer get() throws IOException, 
                        InterruptedException, ErrorCodeException {
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
            return new ClientResponseFuture<Object, ErrorCodeResponse>(null) {
                
                @Override
                public Object get() throws IOException {
                    throw e;
                }
            };
        }
    }
    
    /**
     * @author flangner
     * @since 01/05/2011
     */
    public static final class ErrorCodeException extends Exception {
        private static final long serialVersionUID = 5809888158292614295L;
        
        private final int code;

        public ErrorCodeException(int code) {
            this.code = code;
        }
        
        public int getCode() {
            return code;
        }
        
        /* (non-Javadoc)
         * @see java.lang.Throwable#getMessage()
         */
        @Override
        public String getMessage() {
            return "Operation failed with transmitting-error-code: " + code;
        }
    }
}
