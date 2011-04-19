/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Database;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Databases;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.EntryMap;
import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceClient;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.replication.proxy.ProxyAccessClient;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter.ErrorCodeException;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;
import org.xtreemfs.foundation.pbrpc.client.RPCResponse;

import static org.xtreemfs.babudb.replication.transmission.TransmissionLayer.*;

/**
 * RPCClient for delegating BabuDB requests to the instance with master 
 * privilege.
 * 
 * @author flangner
 * @since 01/19/2011
 */
public class ProxyAccessClientAdapter extends RemoteAccessServiceClient 
    implements ProxyAccessClient {

    public ProxyAccessClientAdapter(RPCNIOSocketClient client) {
        super(client, null);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RemoteAccessClient#makePersistent(
     *  java.net.InetSocketAddress, int, 
     *  org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @Override
    public ClientResponseFuture<Object, ErrorCodeResponse> makePersistent(InetSocketAddress master, 
            int type, ReusableBuffer data) {
        assert (master != null);
        
        try {
            RPCResponse<ErrorCodeResponse> result = makePersistent(master, 
                    AUTHENTICATION, USER_CREDENTIALS, type, data);
            
            return new ClientResponseFuture<Object, ErrorCodeResponse>(result) {
                
                @Override
                public Object resolve(ErrorCodeResponse response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
                    
                    if (response.getErrorCode() != 0) {
                        throw new ErrorCodeException(
                                response.getErrorCode());
                    }
                    return null;
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<Object, ErrorCodeResponse>(null) {
                
                @Override
                public Object resolve(ErrorCodeResponse response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
                    throw e;
                }
            };
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RemoteAccessClient#getDatabase(java.lang.String, 
     *          java.net.InetSocketAddress)
     */
    @Override
    public ClientResponseFuture<Integer,Database> getDatabase(String dbName, 
            InetSocketAddress master) {
 
        assert (master != null);
        
        try {
            RPCResponse<Database> result = getDatabaseByName(master, AUTHENTICATION, 
                    USER_CREDENTIALS, dbName);
            
            return new ClientResponseFuture<Integer,Database>(result) {              

                @Override
                public Integer resolve(Database response, ReusableBuffer data) 
                        throws ErrorCodeException, IOException {
                    return response.getDatabaseId();
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<Integer,Database>(null) {
                
                @Override
                public Integer resolve(Database response, ReusableBuffer data) 
                        throws ErrorCodeException, IOException {
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
    public ClientResponseFuture<Map<String,Integer>, Databases> getDatabases(
            InetSocketAddress master) {
        
        assert (master != null);
        
        try {
            RPCResponse<Databases> result = getDatabases(master, AUTHENTICATION, USER_CREDENTIALS);
            
            return new ClientResponseFuture<Map<String, Integer>, Databases>(result) {
                
                @Override
                public Map<String, Integer> resolve(Databases response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
                    Map<String, Integer> r = new HashMap<String, Integer>();
                    for (Database db : response.getDatabaseList()) {
                        r.put(db.getDatabaseName(), db.getDatabaseId());
                    }
                    return r;
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<Map<String,Integer>, Databases>(null) {
                
                @Override
                public Map<String, Integer> resolve(Databases response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
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
    public ClientResponseFuture<byte[], ErrorCodeResponse> lookup(String dbName, int indexId, 
            ReusableBuffer key, InetSocketAddress master) {

        assert (master != null);
        
        try {
            RPCResponse<ErrorCodeResponse> result = lookup(master, AUTHENTICATION, USER_CREDENTIALS, 
                    dbName, indexId, key);
            
            return new ClientResponseFuture<byte[], ErrorCodeResponse>(result) {
                
                @Override
                public byte[] resolve(ErrorCodeResponse response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
                    
                    int eCode = response.getErrorCode();
                    if (eCode != 0) {
                        throw new ErrorCodeException(eCode);
                    }
                    try {
                        return (data == null) ? null : data.array();
                    } finally {
                        if (data != null) BufferPool.free(data);
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<byte[], ErrorCodeResponse>(null) {
                
                @Override
                public byte[] resolve(ErrorCodeResponse response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
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
    public ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap> 
            prefixLookup(String dbName, int indexId, ReusableBuffer key, 
                    InetSocketAddress master) {

        assert (master != null);
        
        try {
            RPCResponse<EntryMap> result = plookup(master, AUTHENTICATION, USER_CREDENTIALS, dbName, 
                    indexId, key);
            
            return new ClientResponseFuture<ResultSet<byte[], byte[]>,EntryMap>(result) {
                
                @Override
                public ResultSet<byte[], byte[]> resolve(EntryMap response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
                    if (response.getErrorCode() != 0) {
                        throw new ErrorCodeException(response.getErrorCode());
                    }
                    try {
                        int count = response.getLengthCount();
                        assert (count % 2 == 0);
                        
                        Map<byte[], byte[]> m = new HashMap<byte[], byte[]>();
                        
                        int pos = 0;
                        ReusableBuffer k = null;
                        for (int i = 0; i < count; i++) {
                            ReusableBuffer v = data.createViewBuffer();
                            v.position(pos);
                            pos += response.getLength(i);
                            v.limit(pos);
                            
                            if (i % 2 == 0) {
                                k = v;
                            } else {
                                m.put(k.getData(), v.getData());
                                BufferPool.free(k);
                                BufferPool.free(v);
                            }
                        }
                        
                        final Iterator<Entry<byte[],byte[]>> iter = m.entrySet().iterator();
                        return new ResultSet<byte[],byte[]>() {
                            
                            @Override
                            public void remove() {
                                iter.remove();
                            }
                            
                            @Override
                            public Entry<byte[], byte[]> next() {
                                return iter.next();
                            }
                            
                            @Override
                            public boolean hasNext() {
                                return iter.hasNext();
                            }
                            
                            @Override
                            public void free() {}
                        };
                    
                    } finally {
                        BufferPool.free(data);
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap>(null) {
                
                @Override
                public ResultSet<byte[], byte[]> resolve(EntryMap response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
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
    public ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap> prefixLookupR(
            String dbName, int indexId, ReusableBuffer key, 
            InetSocketAddress master) {

        assert (master != null);
        
        try {
            RPCResponse<EntryMap> result = plookupReverse(master, AUTHENTICATION, USER_CREDENTIALS, 
                    dbName, indexId, key);
            
            return new ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap>(result) {
                
                @Override
                public ResultSet<byte[], byte[]> resolve(EntryMap response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
                    
                    if (response.getErrorCode() != 0) {
                        throw new ErrorCodeException(response.getErrorCode());
                    }
                    
                    try {
                        int count = response.getLengthCount();
                        assert (count % 2 == 0);
                        
                        Map<byte[], byte[]> m = new HashMap<byte[], byte[]>();
                        
                        int pos = 0;
                        ReusableBuffer k = null;
                        for (int i = 0; i < count; i++) {
                            ReusableBuffer v = data.createViewBuffer();
                            v.position(pos);
                            pos += response.getLength(i);
                            v.limit(pos);
                            
                            if (i % 2 == 0) {
                                k = v;
                            } else {
                                m.put(k.getData(), v.getData());
                                BufferPool.free(k);
                                BufferPool.free(v);
                            }
                        }
                        
                        final Iterator<Entry<byte[],byte[]>> iter = m.entrySet().iterator();
                        return new ResultSet<byte[],byte[]>() {
                            
                            @Override
                            public void remove() {
                                iter.remove();
                            }
                            
                            @Override
                            public Entry<byte[], byte[]> next() {
                                return iter.next();
                            }
                            
                            @Override
                            public boolean hasNext() {
                                return iter.hasNext();
                            }
                            
                            @Override
                            public void free() {}
                        };
                    
                    } finally {
                        BufferPool.free(data);
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap>(null) {
                
                @Override
                public ResultSet<byte[], byte[]> resolve(EntryMap response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
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
    public ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap> rangeLookup(
            String dbName, int indexId, ReusableBuffer from, ReusableBuffer to, 
            InetSocketAddress master) {
        
        assert (master != null);
        
        ReusableBuffer payload = BufferPool.allocate(from.remaining() + 
                                                     to.remaining());
        payload.put(from);
        payload.put(to);
        
        try {
            RPCResponse<EntryMap> result = rlookup(master, AUTHENTICATION, USER_CREDENTIALS, dbName, 
                    indexId, from.remaining(), payload);
            
            return new ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap>(result) {
                
                @Override
                public ResultSet<byte[], byte[]> resolve(EntryMap response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
                    
                    if (response.getErrorCode() != 0) {
                        throw new ErrorCodeException(response.getErrorCode());
                    }
                    
                    try {
                        int count = response.getLengthCount();
                        assert (count % 2 == 0);
                        
                        Map<byte[], byte[]> m = new HashMap<byte[], byte[]>();
                        
                        int pos = 0;
                        ReusableBuffer k = null;
                        for (int i = 0; i < count; i++) {
                            ReusableBuffer v = data.createViewBuffer();
                            v.position(pos);
                            pos += response.getLength(i);
                            v.limit(pos);
                            
                            if (i % 2 == 0) {
                                k = v;
                            } else {
                                m.put(k.getData(), v.getData());
                                BufferPool.free(k);
                                BufferPool.free(v);
                            }
                        }
                        
                        final Iterator<Entry<byte[],byte[]>> iter = m.entrySet().iterator();
                        return new ResultSet<byte[],byte[]>() {
                            
                            @Override
                            public void remove() {
                                iter.remove();
                            }
                            
                            @Override
                            public Entry<byte[], byte[]> next() {
                                return iter.next();
                            }
                            
                            @Override
                            public boolean hasNext() {
                                return iter.hasNext();
                            }
                            
                            @Override
                            public void free() {}
                        };
                    
                    } finally {
                        BufferPool.free(data);
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap>(null) {
                
                @Override
                public ResultSet<byte[], byte[]> resolve(EntryMap response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
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
    public ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap> rangeLookupR(
            String dbName, int indexId, ReusableBuffer from, ReusableBuffer to, 
            InetSocketAddress master) {
        
        assert (master != null);
        
        ReusableBuffer payload = BufferPool.allocate(from.remaining() + 
                                                     to.remaining());
        payload.put(from);
        payload.put(to);
        
        try {
            RPCResponse<EntryMap> result = rlookupReverse(master, AUTHENTICATION, USER_CREDENTIALS, 
                    dbName, indexId, from.remaining(), payload);
            
            return new ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap>(result) {
                
                @Override
                public ResultSet<byte[], byte[]> resolve(EntryMap response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
                    
                    if (response.getErrorCode() != 0) {
                        throw new ErrorCodeException(response.getErrorCode());
                    }
                    
                    try {
                        int count = response.getLengthCount();
                        assert (count % 2 == 0);
                        
                        Map<byte[], byte[]> m = new HashMap<byte[], byte[]>();
                        
                        int pos = 0;
                        ReusableBuffer k = null;
                        for (int i = 0; i < count; i++) {
                            ReusableBuffer v = data.createViewBuffer();
                            v.position(pos);
                            pos += response.getLength(i);
                            v.limit(pos);
                            
                            if (i % 2 == 0) {
                                k = v;
                            } else {
                                m.put(k.getData(), v.getData());
                                BufferPool.free(k);
                                BufferPool.free(v);
                            }
                        }
                        
                        final Iterator<Entry<byte[],byte[]>> iter = m.entrySet().iterator();
                        return new ResultSet<byte[],byte[]>() {
                            
                            @Override
                            public void remove() {
                                iter.remove();
                            }
                            
                            @Override
                            public Entry<byte[], byte[]> next() {
                                return iter.next();
                            }
                            
                            @Override
                            public boolean hasNext() {
                                return iter.hasNext();
                            }
                            
                            @Override
                            public void free() {}
                        };
                    
                    } finally {
                        BufferPool.free(data);
                    }
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap>(null) {
                
                @Override
                public ResultSet<byte[], byte[]> resolve(EntryMap response, ReusableBuffer data)
                        throws ErrorCodeException, IOException {
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
     * @see org.xtreemfs.babudb.replication.RemoteAccessClient#
     *          getDatabase(int, java.net.InetSocketAddress)
     */
    @Override
    public ClientResponseFuture<String, Database> getDatabase(int dbId, InetSocketAddress master) {

        assert (master != null);
        
        try {
            RPCResponse<Database> result = getDatabaseById(master, AUTHENTICATION, USER_CREDENTIALS, 
                    dbId);
            
            return new ClientResponseFuture<String,Database>(result) {
                
                @Override
                public String resolve(Database response, ReusableBuffer data) 
                        throws ErrorCodeException, IOException {
                    return response.getDatabaseName();
                }
            };
        } catch (final IOException e) {
            return new ClientResponseFuture<String,Database>(null) {
                
                @Override
                public String resolve(Database response, ReusableBuffer data) 
                        throws ErrorCodeException, IOException {
                    throw e;
                }
            };
        }
    }
}