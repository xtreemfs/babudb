/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.database.UserDefinedLookup;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.EntryMap;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.replication.policy.Policy;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.ClientResponseAvailableListener;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter.ErrorCodeException;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

import static org.xtreemfs.babudb.replication.transmission.ErrorCode.*;

/**
 * Stub to redirect Database requests to a remote master if necessary.
 * 
 * @see Policy
 * 
 * @author flangner
 * @since 01/19/2011
 */
public class DatabaseProxy implements DatabaseInternal {

    private final DatabaseManagerProxy  dbMan;
    private final String                name;
    private final int                   id;
    private DatabaseInternal            localDB;
    
    public DatabaseProxy(DatabaseInternal  localDatabase, DatabaseManagerProxy dbManProxy) {
        
        assert (localDatabase != null);
        
        this.localDB = localDatabase;
        this.name = localDB.getName();
        this.id = localDB.getLSMDB().getDatabaseId();
        this.dbMan = dbManProxy;
    }
    
    public DatabaseProxy(String dbName, int dbId, DatabaseManagerProxy dbManProxy) {
        
        this.name = dbName;
        this.id = dbId;
        this.localDB = null;
        this.dbMan = dbManProxy;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#lookup(int, byte[], 
     *          java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<byte[]> lookup(int indexId, byte[] key, 
            final Object context) {
        
        assert (key != null);
        
        InetSocketAddress master = null;
        try {
            master = getServerToPerformAt();
            
            if (master == null) {
                return localDB.lookup(indexId, key, context);
            }
        } catch (final BabuDBException e) {
            return new DatabaseRequestResult<byte[]>() {
                
                @Override
                public void registerListener(
                        DatabaseRequestListener<byte[]> listener) {
                    listener.failed(e, context);
                }
                
                @Override
                public byte[] get() throws BabuDBException {
                    throw e;
                }
            };
        } 
        
        final ClientResponseFuture<byte[], ErrorCodeResponse> r = dbMan.getClient().lookup(name, indexId, 
                ReusableBuffer.wrap(key), master);
        
        return new DatabaseRequestResult<byte[]>() {
            
            @Override
            public void registerListener(
                    final DatabaseRequestListener<byte[]> listener) {
                
                r.registerListener(new ClientResponseAvailableListener<byte[]>() {

                    @Override
                    public void responseAvailable(byte[] rp) {
                        listener.finished(rp, context);
                    }

                    @Override
                    public void requestFailed(Exception e) {
                        BabuDBException be = new BabuDBException(
                                ErrorCode.REPLICATION_FAILURE, e.getMessage());
                        
                        if (e instanceof ErrorCodeException) {
                            be = new BabuDBException(mapTransmissionError(
                                    ((ErrorCodeException) e).getCode()),e.getMessage());
                        }
                            
                        listener.failed(be, context);
                    }
                });
            }
            
            @Override
            public byte[] get() throws BabuDBException {
                try {
                    return r.get();
                } catch (ErrorCodeException ece) {
                    throw new BabuDBException(mapTransmissionError(ece.getCode()),ece.getMessage());
                } catch (Exception e) {
                    throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
                }
            }
        };
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#prefixLookup(int, 
     *          byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> prefixLookup(
            int indexId, byte[] key, final Object context) {
        
        InetSocketAddress master = null;
        try {
            master = getServerToPerformAt();
            
            if (master == null) {
                return localDB.prefixLookup(indexId, key, context);
            }
        } catch (final BabuDBException e) {
            return new DatabaseRequestResult<ResultSet<byte[], byte[]>>() {
                
                @Override
                public void registerListener(
                        DatabaseRequestListener<ResultSet<byte[], byte[]>> listener) {
                    listener.failed(e, context);
                }
                
                @Override
                public ResultSet<byte[], byte[]> get() 
                        throws BabuDBException {
                    throw e;
                }
            };
        }
        
        final ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap> r = 
            dbMan.getClient().prefixLookup(name, indexId, ReusableBuffer.wrap(key), master);
        
        return new DatabaseRequestResult<ResultSet<byte[], byte[]>>() {
            
            @Override
            public void registerListener(
                    final DatabaseRequestListener<ResultSet<byte[], byte[]>> listener) {
                
                r.registerListener(new ClientResponseAvailableListener<ResultSet<byte[], byte[]>>() {

                    @Override
                    public void responseAvailable(ResultSet<byte[], byte[]> rp) {
                        listener.finished(rp, context);
                    }

                    @Override
                    public void requestFailed(Exception e) {                        
                        BabuDBException be = new BabuDBException(
                                ErrorCode.REPLICATION_FAILURE, e.getMessage());
                        
                        if (e instanceof ErrorCodeException) {
                            be = new BabuDBException(mapTransmissionError(
                                    (((ErrorCodeException) e).getCode())),e.getMessage());
                        }
                            
                        listener.failed(be, context);
                    }
                });
            }
            
            @Override
            public ResultSet<byte[], byte[]> get() throws BabuDBException {
                try {
                    return r.get();
                } catch (ErrorCodeException ece) {
                    throw new BabuDBException(mapTransmissionError(ece.getCode()),ece.getMessage());
                } catch (Exception e) {
                    throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
                }
            }
        };
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#reversePrefixLookup(int, 
     *          byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> 
            reversePrefixLookup(int indexId, byte[] key, final Object context) {
        
        InetSocketAddress master = null;
        try {
            master = getServerToPerformAt();
            if (master == null) {
                return localDB.reversePrefixLookup(indexId, key, context);
            }
        } catch (final BabuDBException e) {
            return new DatabaseRequestResult<ResultSet<byte[], byte[]>>() {
                
                @Override
                public void registerListener(
                        DatabaseRequestListener<ResultSet<byte[], byte[]>> listener) {
                    listener.failed(e, context);
                }
                
                @Override
                public ResultSet<byte[], byte[]> get() 
                        throws BabuDBException {
                    throw e;
                }
            };
        }
        
        final ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap> r = 
            dbMan.getClient().prefixLookupR(name, indexId, ReusableBuffer.wrap(key), master);
        
        return new DatabaseRequestResult<ResultSet<byte[], byte[]>>() {
            
            @Override
            public void registerListener(
                    final DatabaseRequestListener<ResultSet<byte[], byte[]>> listener) {
                
                r.registerListener(new ClientResponseAvailableListener<ResultSet<byte[], byte[]>>() {

                    @Override
                    public void responseAvailable(ResultSet<byte[], byte[]> rp) {
                        listener.finished(rp, context);
                    }

                    @Override
                    public void requestFailed(Exception e) {
                        BabuDBException be = new BabuDBException(
                                ErrorCode.REPLICATION_FAILURE, e.getMessage());
                        
                        if (e instanceof ErrorCodeException) {
                            be = new BabuDBException(mapTransmissionError(
                                    ((ErrorCodeException) e).getCode()),e.getMessage());
                        }
                            
                        listener.failed(be, context);
                    }
                });
            }
            
            @Override
            public ResultSet<byte[], byte[]> get() throws BabuDBException {
                try {
                    return r.get();
                } catch (ErrorCodeException ece) {
                    throw new BabuDBException(mapTransmissionError(ece.getCode()),ece.getMessage());
                } catch (Exception e) {
                    throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
                }
            }
        };
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#rangeLookup(int, byte[], 
     *          byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> rangeLookup(
            int indexId, byte[] from, byte[] to, final Object context) {
        
        InetSocketAddress master = null;
        try {
            master = getServerToPerformAt();
            if (master == null) {
                return localDB.rangeLookup(indexId, from, to, context);
            }
        } catch (final BabuDBException e) {
            return new DatabaseRequestResult<ResultSet<byte[], byte[]>>() {
                
                @Override
                public void registerListener(
                        DatabaseRequestListener<ResultSet<byte[], byte[]>> listener) {
                    listener.failed(e, context);
                }
                
                @Override
                public ResultSet<byte[], byte[]> get() 
                        throws BabuDBException {
                    throw e;
                }
            };
        }
        
        final ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap> r = 
            dbMan.getClient().rangeLookup(name, indexId, ReusableBuffer.wrap(from), 
                    ReusableBuffer.wrap(to), master);
        
        return new DatabaseRequestResult<ResultSet<byte[], byte[]>>() {
            
            @Override
            public void registerListener(
                    final DatabaseRequestListener<ResultSet<byte[], byte[]>> listener) {
                
                r.registerListener(new ClientResponseAvailableListener<ResultSet<byte[], byte[]>>() {

                    @Override
                    public void responseAvailable(ResultSet<byte[], byte[]> rp) {
                        listener.finished(rp, context);
                    }

                    @Override
                    public void requestFailed(Exception e) {
                        BabuDBException be = new BabuDBException(
                                ErrorCode.REPLICATION_FAILURE, e.getMessage());
                        
                        if (e instanceof ErrorCodeException) {
                            be = new BabuDBException(mapTransmissionError(
                                    ((ErrorCodeException) e).getCode()),e.getMessage());
                        }
                            
                        listener.failed(be, context);
                    }
                });
            }
            
            @Override
            public ResultSet<byte[], byte[]> get() throws BabuDBException {
                try {
                    return r.get();
                } catch (ErrorCodeException ece) {
                    throw new BabuDBException(mapTransmissionError(ece.getCode()),ece.getMessage());
                } catch (Exception e) {
                    throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
                }
            }
        };
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#reverseRangeLookup(int, 
     *          byte[], byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> 
            reverseRangeLookup(int indexId, byte[] from, byte[] to, 
            final Object context) {
        
        InetSocketAddress master = null;
        try {
            master = getServerToPerformAt();
            if (master == null) {
                return localDB.reverseRangeLookup(indexId, from, to, context);
            }
        } catch (final BabuDBException e) {
            return new DatabaseRequestResult<ResultSet<byte[], byte[]>>() {
                
                @Override
                public void registerListener(
                        DatabaseRequestListener<ResultSet<byte[], byte[]>> listener) {
                    listener.failed(e, context);
                }
                
                @Override
                public ResultSet<byte[], byte[]> get() 
                        throws BabuDBException {
                    throw e;
                }
            };
        }
        
        final ClientResponseFuture<ResultSet<byte[], byte[]>, EntryMap> r = 
            dbMan.getClient().rangeLookupR(name, indexId, 
                    ReusableBuffer.wrap(from), ReusableBuffer.wrap(to), master);
        
        return new DatabaseRequestResult<ResultSet<byte[], byte[]>>() {
            
            @Override
            public void registerListener(
                    final DatabaseRequestListener<ResultSet<byte[], byte[]>> listener) {
                
                r.registerListener(new ClientResponseAvailableListener<ResultSet<byte[], byte[]>>() {

                    @Override
                    public void responseAvailable(ResultSet<byte[], byte[]> rp) {
                        listener.finished(rp, context);
                    }

                    @Override
                    public void requestFailed(Exception e) {
                        BabuDBException be = new BabuDBException(
                                ErrorCode.REPLICATION_FAILURE, e.getMessage());
                        
                        if (e instanceof ErrorCodeException) {
                            be = new BabuDBException(mapTransmissionError(
                                    ((ErrorCodeException) e).getCode()),e.getMessage());
                        }
                            
                        listener.failed(be, context);
                    }
                });
            }
            
            @Override
            public ResultSet<byte[], byte[]> get() throws BabuDBException {
                try {
                    return r.get();
                } catch (ErrorCodeException ece) {
                    throw new BabuDBException(mapTransmissionError(ece.getCode()),ece.getMessage());
                } catch (Exception e) {
                    throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
                }
            }
        };
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#userDefinedLookup(
     *          org.xtreemfs.babudb.api.database.UserDefinedLookup, 
     *          java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Object> userDefinedLookup(
            UserDefinedLookup udl, final Object context) {
        
        InetSocketAddress master = null;
        try {
            
            master = getServerToPerformAt();
            if (master == null) {
                return localDB.userDefinedLookup(udl, context);
            }
        } catch (final BabuDBException e) {
            return new DatabaseRequestResult<Object>() {
                
                @Override
                public void registerListener(
                        DatabaseRequestListener<Object> listener) {
                    listener.failed(e, context);
                }
                
                @Override
                public Object get() 
                        throws BabuDBException {
                    throw e;
                }
            };
        }
        
        // TODO RPC: userLookup return(Object ... will be hard to serialize)
        throw new UnsupportedOperationException("This operation is " +
        		"not supported by the replication-plugin yet.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#shutdown()
     */
    @Override
    public void shutdown() throws BabuDBException {
        
        if (localDB != null) {
            localDB.shutdown();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#getName()
     */
    @Override
    public String getName() {
        return name;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#createInsertGroup()
     */
    @Override
    public BabuDBInsertGroup createInsertGroup() {
        return BabuDBInsertGroup.createInsertGroup(id);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#getComparators()
     */
    @Override
    public ByteRangeComparator[] getComparators() {
        
        InetSocketAddress master = null;
        try {
            
            master = getServerToPerformAt();
            if (master == null) {
                return localDB.getComparators();
            }
        } catch (BabuDBException e) { /* ignored */ }
        
        // TODO RPC: ByteRangeComparators have to be serializable 
        throw new UnsupportedOperationException("This operation is " +
                "not supported by the replication-plugin yet.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#singleInsert(int, byte[], 
     *          byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Object> singleInsert(int indexId, byte[] key, 
            byte[] value, Object context) {
        
        DatabaseInsertGroup irg = createInsertGroup();
        irg.addInsert(indexId, key, value);
        return insert(irg, context);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#insert(
     *          org.xtreemfs.babudb.api.database.DatabaseInsertGroup, java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Object> insert(DatabaseInsertGroup irg, Object context) {
        return insert((BabuDBInsertGroup) irg, context);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#insert(
     *          org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup, java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Object> insert(BabuDBInsertGroup irg, Object context) {
        
        BabuDBRequestResultImpl<Object> result = 
            new BabuDBRequestResultImpl<Object>(context, dbMan.getResponseManager());
        
        try {
            dbMan.getTransactionManager().makePersistent(
                    dbMan.createTransaction().insertRecordGroup(getName(), irg.getRecord()), result);
        } catch (BabuDBException e) {
            result.failed(e);
        }
        
        return result;
    }

    /**
     * @return the host to perform the request at, or null, if it is permitted to perform the 
     *         request locally.
     * @throws BabuDBException if replication is currently not available.
     */
    private InetSocketAddress getServerToPerformAt() throws BabuDBException {
        
        InetSocketAddress master;
        try {
            master = dbMan.getReplicationManager().getMaster();
        } catch (InterruptedException e) {
            throw new BabuDBException(ErrorCode.INTERRUPTED, 
                "Waiting for a lease holder was interrupted.", e);
        }
        
        boolean isMaster = dbMan.getReplicationManager().isItMe(master);
        
        if (isMaster || !dbMan.getReplicationPolicy().lookUpIsMasterRestricted()) {
                
            // this service is allowed to retrieve the local database for lookups
            if (isMaster || !dbMan.getReplicationPolicy().dbModificationIsMasterRestricted()) {
                localDB = dbMan.getLocalDatabase(name);
                
            // existence of the database has to be verified at the master
            } else {
                try {
                    dbMan.getClient().getDatabase(name, master).get();
                    localDB = dbMan.getLocalDatabase(name);
                } catch (BabuDBException e) {
                    throw e;
                } catch (ErrorCodeException e) {
                    throw new BabuDBException(mapTransmissionError(e.getCode()), e.getMessage(), e);
                } catch (Exception e) {
                    throw new BabuDBException(ErrorCode.IO_ERROR, e.getMessage(), e);
                }
            }
             
            return null;
        }

        return master;
    }
    
/*
 * TODO is it really necessary to forbid usage of internal mechanisms provided by databases?
 */

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#getLSMDB()
     */
    @Override
    public LSMDatabase getLSMDB() {
        try {
            if (getServerToPerformAt() == null) {
                return localDB.getLSMDB();
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        throw new UnsupportedOperationException("Internally manipulating a Database of a " 
                + "'not master' server is not supported by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#proceedWriteSnapshot(int[], 
     *          java.lang.String, org.xtreemfs.babudb.snapshots.SnapshotConfig)
     */
    @Override
    public void proceedWriteSnapshot(int[] snapIds, String directory, SnapshotConfig cfg)
            throws BabuDBException {
        boolean permission = false;
        try {
            if (getServerToPerformAt() == null) {
                permission = true;
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        if (permission) {
            localDB.proceedWriteSnapshot(snapIds, directory, cfg);
        } else {
            throw new UnsupportedOperationException("Internally manipulating a Database of a " 
                    + "'not master' server is not supported by the replication plugin.");
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#setLSMDB(org.xtreemfs.babudb.lsmdb.LSMDatabase)
     */
    @Override
    public void setLSMDB(LSMDatabase lsmDatabase) {
        try {
            if (getServerToPerformAt() == null) {
                localDB.setLSMDB(lsmDatabase);
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        throw new UnsupportedOperationException("Internally manipulating a Database of a " 
                + "'not master' server is not supported by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#proceedWriteSnapshot(int, long, int[])
     */
    @Override
    public void proceedWriteSnapshot(int viewId, long sequenceNo, int[] snapIds) throws BabuDBException {
        boolean permission = false;
        try {
            if (getServerToPerformAt() == null) {
                permission = true;
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        if (permission) {
            localDB.proceedWriteSnapshot(viewId, sequenceNo, snapIds);
        } else {
            throw new UnsupportedOperationException("Internally manipulating a Database of a " 
                    + "'not master' server is not supported by the replication plugin.");
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#proceedCleanupSnapshot(int, long)
     */
    @Override
    public void proceedCleanupSnapshot(int viewId, long sequenceNo) throws BabuDBException {
        boolean permission = false;
        try {
            if (getServerToPerformAt() == null) {
                permission = true;
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        if (permission) {
            localDB.proceedCleanupSnapshot(viewId, sequenceNo);
        } else {
            throw new UnsupportedOperationException("Internally manipulating a Database of a " 
                    + "'not master' server is not supported by the replication plugin.");
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#proceedCreateSnapshot()
     */
    @Override
    public int[] proceedCreateSnapshot() {
        try {
            if (getServerToPerformAt() == null) {
                return localDB.proceedCreateSnapshot();
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        throw new UnsupportedOperationException("Internally manipulating a Database of a " 
                + "'not master' server is not supported by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#dumpSnapshot(java.lang.String)
     */
    @Override
    public void dumpSnapshot(String baseDir) throws BabuDBException {
        boolean permission = false;
        try {
            if (getServerToPerformAt() == null) {
                permission = true;
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        if (permission) {
            localDB.dumpSnapshot(baseDir);
        } else {
            throw new UnsupportedOperationException("Internally manipulating a Database of a " 
                    + "'not master' server is not supported by the replication plugin.");
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#proceedSnapshot(java.lang.String)
     */
    @Override
    public void proceedSnapshot(String destDB) throws BabuDBException, InterruptedException {
        boolean permission = false;
        try {
            if (getServerToPerformAt() == null) {
                permission = true;
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        if (permission) {
            localDB.proceedSnapshot(destDB);
        } else {
            throw new UnsupportedOperationException("Internally manipulating a Database of a " 
                    + "'not master' server is not supported by the replication plugin.");
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#directLookup(int, int, byte[])
     */
    @Override
    public byte[] directLookup(int indexId, int snapId, byte[] key) throws BabuDBException {
        boolean permission = false;
        try {
            if (getServerToPerformAt() == null) {
                permission = true;
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        if (permission) {
            return localDB.directLookup(indexId, snapId, key);
        } else {
            throw new UnsupportedOperationException("Internally manipulating a Database of a " 
                    + "'not master' server is not supported by the replication plugin.");
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#directPrefixLookup(int, int, byte[], boolean)
     */
    @Override
    public ResultSet<byte[], byte[]> directPrefixLookup(int indexId, int snapId, byte[] key, boolean ascending)
            throws BabuDBException {
        
        boolean permission = false;
        try {
            if (getServerToPerformAt() == null) {
                permission = true;
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        if (permission) {
            return localDB.directPrefixLookup(indexId, snapId, key, ascending);
        } else {
            throw new UnsupportedOperationException("Internally manipulating a Database of a " 
                    + "'not master' server is not supported by the replication plugin.");
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#directRangeLookup(int, int, byte[], byte[], boolean)
     */
    @Override
    public ResultSet<byte[], byte[]> directRangeLookup(int indexId, int snapId, byte[] from, byte[] to,
            boolean ascending) throws BabuDBException {
        
        boolean permission = false;
        try {
            if (getServerToPerformAt() == null) {
                permission = true;
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        if (permission) {
            return localDB.directRangeLookup(indexId, snapId, from, to, ascending);
        } else {
            throw new UnsupportedOperationException("Internally manipulating a Database of a " 
                    + "'not master' server is not supported by the replication plugin.");
        }
    }
}
