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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.xtreemfs.babudb.pbrpc.GlobalTypes.Database;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Databases;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.EntryMap;
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
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RemoteAccessClient#getDatabase(java.lang.String, 
     *          java.net.InetSocketAddress)
     */
    @Override
    public ClientResponseFuture<Integer> getDatabase(String dbName, InetSocketAddress master) {
 
        assert (master != null);
        
        try {
            final RPCResponse<Database> result = getDatabase(master, 
                    Auth.getDefaultInstance(), 
                    UserCredentials.getDefaultInstance(), dbName);
            
            return new ClientResponseFuture<Integer>(result) {
                
                @Override
                public Integer get() throws ErrorCodeException, 
                        IOException, InterruptedException {
                    return result.get().getDatabaseId();
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<Integer>(null) {
                
                @Override
                public Integer get() throws ErrorCodeException, 
                        IOException, InterruptedException {
                    throw e;
                }
            };
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RemoteAccessClient#getDatabases(
     *          java.net.InetSocketAddress)
     */
    @Override
    public ClientResponseFuture<Map<String,Integer>> getDatabases(
            InetSocketAddress master) {
        
        assert (master != null);
        
        try {
            final RPCResponse<Databases> result = getDatabases(master, 
                    Auth.getDefaultInstance(), 
                    UserCredentials.getDefaultInstance());
            
            return new ClientResponseFuture<Map<String, Integer>>(result) {
                
                @Override
                public Map<String, Integer> get() throws ErrorCodeException, 
                        IOException, InterruptedException {
                    Map<String, Integer> r = new HashMap<String, Integer>();
                    Databases dbs = result.get();
                    for (Database db : dbs.getDatabaseList()) {
                        r.put(db.getDatabaseName(), db.getDatabaseId());
                    }
                    return r;
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<Map<String,Integer>>(null) {
                
                @Override
                public Map<String, Integer> get() throws ErrorCodeException, 
                        IOException, InterruptedException {
                    throw e;
                }
            };
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RemoteAccessClient#lookup(
     *     java.lang.String, int, org.xtreemfs.foundation.buffer.ReusableBuffer, 
     *     java.net.InetSocketAddress)
     */
    @Override
    public ClientResponseFuture<byte[]> lookup(String dbName, int indexId, 
            ReusableBuffer key, InetSocketAddress master) {

        assert (master != null);
        
        try {
            final RPCResponse<ErrorCodeResponse> result = lookup(master, 
                    Auth.getDefaultInstance(), 
                    UserCredentials.getDefaultInstance(), dbName, indexId, key);
            
            return new ClientResponseFuture<byte[]>(result) {
                
                @Override
                public byte[] get() throws ErrorCodeException, 
                        IOException, InterruptedException {
                    int eCode = result.get().getErrorCode();
                    if (eCode != 0) {
                        throw new ErrorCodeException(eCode);
                    }
                    ReusableBuffer data = result.getData();
                    try {
                        return data.array();
                    } finally {
                        BufferPool.free(data);
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<byte[]>(null) {
                
                @Override
                public byte[] get() throws ErrorCodeException, 
                        IOException, InterruptedException {
                    throw e;
                }
            };
        } finally {
            BufferPool.free(key);
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RemoteAccessClient#prefixLookup(
     *          java.lang.String, int, 
     *          org.xtreemfs.foundation.buffer.ReusableBuffer, 
     *          java.net.InetSocketAddress)
     */
    @Override
    public ClientResponseFuture<Iterator<Entry<byte[], byte[]>>> 
            prefixLookup(String dbName, int indexId, ReusableBuffer key, 
                    InetSocketAddress master) {

        assert (master != null);
        
        try {
            final RPCResponse<EntryMap> result = plookup(master, 
                    Auth.getDefaultInstance(), 
                    UserCredentials.getDefaultInstance(), dbName, indexId, key);
            
            return new ClientResponseFuture<Iterator<Entry<byte[], byte[]>>>(result) {
                
                @Override
                public Iterator<Entry<byte[], byte[]>> get() 
                        throws ErrorCodeException, IOException, 
                        InterruptedException {
                    
                    EntryMap e = result.get();
                    if (e.getErrorCode() != 0) {
                        throw new ErrorCodeException(e.getErrorCode());
                    }
                    ReusableBuffer data = result.getData();
                    
                    try {
                        int count = e.getLengthCount();
                        assert (count % 2 == 0);
                        
                        Map<byte[], byte[]> m = new HashMap<byte[], byte[]>();
                        
                        int pos = 0;
                        ReusableBuffer k = null;
                        for (int i = 0; i < count; i++) {
                            ReusableBuffer v = 
                                result.getData().createViewBuffer();
                            v.position(pos);
                            pos += e.getLength(i);
                            v.limit(pos);
                            
                            if (i % 2 == 0) {
                                k = v;
                            } else {
                                m.put(k.getData(), v.getData());
                                BufferPool.free(k);
                                BufferPool.free(v);
                            }
                        }
                        return m.entrySet().iterator();
                    
                    } finally {
                        BufferPool.free(data);
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<Iterator<Entry<byte[], byte[]>>>(null) {
                
                @Override
                public Iterator<Entry<byte[], byte[]>> get() 
                        throws ErrorCodeException, IOException, 
                        InterruptedException {
                    throw e;
                }
            };
        } finally {
            BufferPool.free(key);
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RemoteAccessClient#prefixLookupR(
     *          java.lang.String, int, org.xtreemfs.foundation.buffer.ReusableBuffer, 
     *          java.net.InetSocketAddress)
     */
    @Override
    public ClientResponseFuture<Iterator<Entry<byte[], byte[]>>> prefixLookupR(
            String dbName, int indexId, ReusableBuffer key, 
            InetSocketAddress master) {

        assert (master != null);
        
        try {
            final RPCResponse<EntryMap> result = plookupReverse(master, 
                    Auth.getDefaultInstance(), 
                    UserCredentials.getDefaultInstance(), dbName, indexId, key);
            
            return new ClientResponseFuture<Iterator<Entry<byte[], byte[]>>>(result) {
                
                @Override
                public Iterator<Entry<byte[], byte[]>> get() 
                        throws ErrorCodeException, IOException, 
                        InterruptedException {
                    
                    EntryMap e = result.get();
                    if (e.getErrorCode() != 0) {
                        throw new ErrorCodeException(e.getErrorCode());
                    }
                    ReusableBuffer data = result.getData();
                    
                    try {
                        int count = e.getLengthCount();
                        assert (count % 2 == 0);
                        
                        Map<byte[], byte[]> m = new HashMap<byte[], byte[]>();
                        
                        int pos = 0;
                        ReusableBuffer k = null;
                        for (int i = 0; i < count; i++) {
                            ReusableBuffer v = 
                                result.getData().createViewBuffer();
                            v.position(pos);
                            pos += e.getLength(i);
                            v.limit(pos);
                            
                            if (i % 2 == 0) {
                                k = v;
                            } else {
                                m.put(k.getData(), v.getData());
                                BufferPool.free(k);
                                BufferPool.free(v);
                            }
                        }
                        return m.entrySet().iterator();
                    
                    } finally {
                        BufferPool.free(data);
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<Iterator<Entry<byte[], byte[]>>>(null) {
                
                @Override
                public Iterator<Entry<byte[], byte[]>> get() 
                        throws ErrorCodeException, IOException, 
                        InterruptedException {
                    throw e;
                }
            };
        } finally {
            BufferPool.free(key);
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RemoteAccessClient#rangeLookup(
     *          java.lang.String, int, org.xtreemfs.foundation.buffer.ReusableBuffer, 
     *          org.xtreemfs.foundation.buffer.ReusableBuffer, java.net.InetSocketAddress)
     */
    @Override
    public ClientResponseFuture<Iterator<Entry<byte[], byte[]>>> rangeLookup(
            String dbName, int indexId, ReusableBuffer from, ReusableBuffer to, 
            InetSocketAddress master) {
        
        assert (master != null);
        
        ReusableBuffer payload = BufferPool.allocate(from.remaining() + 
                                                     to.remaining());
        payload.put(from);
        payload.put(to);
        
        try {
            final RPCResponse<EntryMap> result = rlookup(master, 
                    Auth.getDefaultInstance(), 
                    UserCredentials.getDefaultInstance(), dbName, indexId,  
                    from.remaining(), payload);
            
            return new ClientResponseFuture<Iterator<Entry<byte[], byte[]>>>(result) {
                
                @Override
                public Iterator<Entry<byte[], byte[]>> get() 
                        throws ErrorCodeException, IOException, 
                        InterruptedException {
                    
                    EntryMap e = result.get();
                    if (e.getErrorCode() != 0) {
                        throw new ErrorCodeException(e.getErrorCode());
                    }
                    ReusableBuffer data = result.getData();
                    
                    try {
                        int count = e.getLengthCount();
                        assert (count % 2 == 0);
                        
                        Map<byte[], byte[]> m = new HashMap<byte[], byte[]>();
                        
                        int pos = 0;
                        ReusableBuffer k = null;
                        for (int i = 0; i < count; i++) {
                            ReusableBuffer v = 
                                result.getData().createViewBuffer();
                            v.position(pos);
                            pos += e.getLength(i);
                            v.limit(pos);
                            
                            if (i % 2 == 0) {
                                k = v;
                            } else {
                                m.put(k.getData(), v.getData());
                                BufferPool.free(k);
                                BufferPool.free(v);
                            }
                        }
                        return m.entrySet().iterator();
                    
                    } finally {
                        BufferPool.free(data);
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<Iterator<Entry<byte[], byte[]>>>(null) {
                
                @Override
                public Iterator<Entry<byte[], byte[]>> get() 
                        throws ErrorCodeException, IOException, 
                        InterruptedException {
                    throw e;
                }
            };
        } finally {
            BufferPool.free(from);
            BufferPool.free(to);
            BufferPool.free(payload);
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RemoteAccessClient#rangeLookupR(
     *          java.lang.String, int, org.xtreemfs.foundation.buffer.ReusableBuffer, 
     *          org.xtreemfs.foundation.buffer.ReusableBuffer, java.net.InetSocketAddress)
     */
    @Override
    public ClientResponseFuture<Iterator<Entry<byte[], byte[]>>> rangeLookupR(
            String dbName, int indexId, ReusableBuffer from, ReusableBuffer to, 
            InetSocketAddress master) {
        
        assert (master != null);
        
        ReusableBuffer payload = BufferPool.allocate(from.remaining() + 
                                                     to.remaining());
        payload.put(from);
        payload.put(to);
        
        try {
            final RPCResponse<EntryMap> result = rlookupReverse(master, 
                    Auth.getDefaultInstance(), 
                    UserCredentials.getDefaultInstance(), dbName, indexId,  
                    from.remaining(), payload);
            
            return new ClientResponseFuture<Iterator<Entry<byte[], byte[]>>>(result) {
                
                @Override
                public Iterator<Entry<byte[], byte[]>> get() 
                        throws ErrorCodeException, IOException, 
                        InterruptedException {
                    
                    EntryMap e = result.get();
                    if (e.getErrorCode() != 0) {
                        throw new ErrorCodeException(e.getErrorCode());
                    }
                    ReusableBuffer data = result.getData();
                    
                    try {
                        int count = e.getLengthCount();
                        assert (count % 2 == 0);
                        
                        Map<byte[], byte[]> m = new HashMap<byte[], byte[]>();
                        
                        int pos = 0;
                        ReusableBuffer k = null;
                        for (int i = 0; i < count; i++) {
                            ReusableBuffer v = 
                                result.getData().createViewBuffer();
                            v.position(pos);
                            pos += e.getLength(i);
                            v.limit(pos);
                            
                            if (i % 2 == 0) {
                                k = v;
                            } else {
                                m.put(k.getData(), v.getData());
                                BufferPool.free(k);
                                BufferPool.free(v);
                            }
                        }
                        return m.entrySet().iterator();
                    
                    } finally {
                        BufferPool.free(data);
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<Iterator<Entry<byte[], byte[]>>>(null) {
                
                @Override
                public Iterator<Entry<byte[], byte[]>> get() 
                        throws ErrorCodeException, IOException, 
                        InterruptedException {
                    throw e;
                }
            };
        } finally {
            BufferPool.free(from);
            BufferPool.free(to);
            BufferPool.free(payload);
        }
    }
}
