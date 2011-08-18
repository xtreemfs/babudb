/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.lsmdb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.dev.DatabaseManagerInternal;
import org.xtreemfs.babudb.api.dev.transaction.InMemoryProcessing;
import org.xtreemfs.babudb.api.dev.transaction.OperationInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.api.transaction.Operation;
import org.xtreemfs.babudb.api.transaction.Transaction;
import org.xtreemfs.babudb.api.transaction.TransactionListener;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup.InsertRecord;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.FSUtils;

public class DatabaseManagerImpl implements DatabaseManagerInternal {
    
    private static final String                    RUNTIME_STATE_DBCREATIONCOUNT = "databaseManager.dbCreationCount";
    private static final String                    RUNTIME_STATE_DBDELETIONCOUNT = "databaseManager.dbDeletionCount";
    
    private BabuDBInternal                         dbs;
    
    /**
     * Mapping from database name to database id
     */
    private final Map<String, DatabaseInternal>    dbsByName;
    
    /**
     * Mapping from dbId to database
     */
    private final Map<Integer, DatabaseInternal>   dbsById;
    
    /**
     * a map containing all comparators sorted by their class names
     */
    private final Map<String, ByteRangeComparator> compInstances;
    
    /**
     * ID to assign to next database create
     */
    private int                                    nextDbId;
    
    /**
     * object used for synchronizing modifications of the database lists.
     */
    private final Object                           dbModificationLock;
    
    private AtomicInteger                          _dbCreationCount              = new AtomicInteger();
    
    private AtomicInteger                          _dbDeletionCount              = new AtomicInteger();
    
    public DatabaseManagerImpl(BabuDBInternal dbs) throws BabuDBException {
        
        this.dbs = dbs;
        
        this.dbsByName = new HashMap<String, DatabaseInternal>();
        this.dbsById = new HashMap<Integer, DatabaseInternal>();
        
        this.compInstances = new HashMap<String, ByteRangeComparator>();
        this.compInstances.put(DefaultByteRangeComparator.class.getName(), new DefaultByteRangeComparator());
        
        this.nextDbId = 1;
        this.dbModificationLock = new Object();
        
        initializeTransactionManager();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#reset()
     */
    @Override
    public void reset() throws BabuDBException {
        nextDbId = 1;
        
        compInstances.clear();
        compInstances.put(DefaultByteRangeComparator.class.getName(), new DefaultByteRangeComparator());
        
        dbs.getDBConfigFile().reset();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.DatabaseManager#getDatabases()
     */
    @Override
    public Map<String, Database> getDatabases() {
        return new HashMap<String, Database>(getDatabasesInternal());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDatabasesInternal
     * ()
     */
    @Override
    public Map<String, DatabaseInternal> getDatabasesInternal() {
        synchronized (dbModificationLock) {
            return new HashMap<String, DatabaseInternal>(dbsByName);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDatabaseList()
     */
    @Override
    public Collection<DatabaseInternal> getDatabaseList() {
        synchronized (dbModificationLock) {
            return new ArrayList<DatabaseInternal>(dbsById.values());
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDatabase(java.
     * lang.String)
     */
    @Override
    public DatabaseInternal getDatabase(String dbName) throws BabuDBException {
        
        DatabaseInternal db = dbsByName.get(dbName);
        
        if (db == null) {
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database with name " + dbName + " does not exist");
        }
        return db;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDatabase(int)
     */
    @Override
    public DatabaseInternal getDatabase(int dbId) throws BabuDBException {
        DatabaseInternal db = dbsById.get(dbId);
        
        if (db == null) {
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database (" + dbId + ") does not exist");
        }
        return db;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#createDatabase(java
     * .lang.String, int)
     */
    @Override
    public DatabaseInternal createDatabase(String databaseName, int numIndices) throws BabuDBException {
        return createDatabase(databaseName, numIndices, null);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#createDatabase(java
     * .lang.String, int, org.xtreemfs.babudb.api.index.ByteRangeComparator[])
     */
    @Override
    public DatabaseInternal createDatabase(String databaseName, int numIndices, ByteRangeComparator[] comparators)
            throws BabuDBException {
        
        BabuDBRequestResultImpl<Object> result = new BabuDBRequestResultImpl<Object>(dbs.getResponseManager());
        dbs.getTransactionManager().makePersistent(
                createTransaction().createDatabase(databaseName, numIndices, comparators), result);
        Object obj = ((Object[]) result.get())[0];
        
        _dbCreationCount.incrementAndGet();
        
        return (DatabaseInternal) obj;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.DatabaseManager#deleteDatabase(java.lang.String)
     */
    @Override
    public void deleteDatabase(String databaseName) throws BabuDBException {
        BabuDBRequestResultImpl<Object> result = new BabuDBRequestResultImpl<Object>(dbs.getResponseManager());
        dbs.getTransactionManager().makePersistent(createTransaction().deleteDatabase(databaseName), result);
        result.get();
        
        _dbDeletionCount.incrementAndGet();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.DatabaseManager#copyDatabase(java.lang.String,
     * java.lang.String)
     */
    @Override
    public void copyDatabase(String sourceDB, String destDB) throws BabuDBException {
        BabuDBRequestResultImpl<Object> result = new BabuDBRequestResultImpl<Object>(dbs.getResponseManager());
        dbs.getTransactionManager().makePersistent(createTransaction().copyDatabase(sourceDB, destDB), result);
        result.get();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#shutdown()
     */
    @Override
    public void shutdown() throws BabuDBException {
        for (Database db : dbsById.values())
            db.shutdown();
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "DB manager shut down successfully");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDBModificationLock
     * ()
     */
    @Override
    public Object getDBModificationLock() {
        return dbModificationLock;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.DatabaseManager#dumpAllDatabases(java.lang.String
     * )
     */
    @Override
    public void dumpAllDatabases(String destPath) throws BabuDBException, InterruptedException, IOException {
        // create a snapshot of each database materialized with the destPath as
        // baseDir
        destPath = destPath.endsWith(File.separator) ? destPath : destPath + File.separator;
        
        File dir = new File(destPath);
        if (!dir.exists() && !dir.mkdirs())
            throw new IOException("Directory doesnt exist and cannot be created:'" + destPath + "'");
        
        BabuDBConfig cfg = dbs.getConfig();
        dbs.getDBConfigFile().save(destPath + cfg.getDbCfgFile());
        
        for (DatabaseInternal db : dbsByName.values()) {
            db.dumpSnapshot(destPath);
        }
    }
    
    /**
     * Feed the transactionManager with the knowledge to handle
     * database-modifying related requests.
     */
    private void initializeTransactionManager() {
        
        dbs.getTransactionManager().registerInMemoryProcessing(Operation.TYPE_CREATE_DB, new InMemoryProcessing() {
            
            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {
                
                serialized.getInt(); // do not use, deprecated
                
                String dbName = serialized.getString();
                int indices = serialized.getInt();
                
                serialized.flip();
                
                return new Object[] { dbName, indices, null };
            }
            
            @Override
            public OperationInternal convertToOperation(Object[] args) {
                return new BabuDBTransaction.BabuDBOperation(Operation.TYPE_CREATE_DB, (String) args[0], new Object[] {
                        args[1], args[2] });
            }
            
            @Override
            public DatabaseInternal process(OperationInternal operation) throws BabuDBException {
                // parse args
                Object[] args = operation.getParams();
                int numIndices = (Integer) args[0];
                
                ByteRangeComparator[] com = null;
                if (args.length > 2) {
                    com = (ByteRangeComparator[]) args[1];
                }
                if (com == null) {
                    ByteRangeComparator[] comps = new ByteRangeComparator[numIndices];
                    final ByteRangeComparator defaultComparator = compInstances.get(DefaultByteRangeComparator.class
                            .getName());
                    for (int i = 0; i < numIndices; i++) {
                        comps[i] = defaultComparator;
                    }
                    
                    com = comps;
                }
                
                DatabaseImpl db = null;
                synchronized (getDBModificationLock()) {
                    synchronized (dbs.getCheckpointer()) {
                        if (dbsByName.containsKey(operation.getDatabaseName())) {
                            throw new BabuDBException(ErrorCode.DB_EXISTS, "database '" + operation.getDatabaseName()
                                    + "' already exists");
                        }
                        final int dbId = nextDbId++;
                        db = new DatabaseImpl(dbs, new LSMDatabase(operation.getDatabaseName(), dbId, dbs.getConfig()
                                .getBaseDir() + operation.getDatabaseName() + File.separatorChar, numIndices, false,
                                com, dbs.getConfig().getCompression(), dbs.getConfig().getMaxNumRecordsPerBlock(), dbs
                                        .getConfig().getMaxBlockFileSize(), dbs.getConfig().getDisableMMap(), dbs
                                        .getConfig().getMMapLimit()));
                        dbsById.put(dbId, db);
                        dbsByName.put(operation.getDatabaseName(), db);
                        dbs.getDBConfigFile().save();
                    }
                }
                
                return db;
            }
        });
        
        dbs.getTransactionManager().registerInMemoryProcessing(Operation.TYPE_DELETE_DB, new InMemoryProcessing() {
            
            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {
                
                serialized.getInt(); // do not use, deprecated
                
                String dbName = serialized.getString();
                serialized.flip();
                
                return new Object[] { dbName };
            }
            
            @Override
            public OperationInternal convertToOperation(Object[] args) {
                return new BabuDBTransaction.BabuDBOperation(Operation.TYPE_DELETE_DB, (String) args[0], null);
            }
            
            @Override
            public Object process(OperationInternal operation) throws BabuDBException {
                
                int dbId = InsertRecordGroup.DB_ID_UNKNOWN;
                synchronized (getDBModificationLock()) {
                    synchronized (dbs.getCheckpointer()) {
                        if (!dbsByName.containsKey(operation.getDatabaseName())) {
                            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '" + operation.getDatabaseName()
                                    + "' does not exists");
                        }
                        final LSMDatabase db = getDatabase(operation.getDatabaseName()).getLSMDB();
                        dbId = db.getDatabaseId();
                        dbsByName.remove(operation.getDatabaseName());
                        dbsById.remove(dbId);
                        
                        dbs.getSnapshotManager().deleteAllSnapshots(operation.getDatabaseName());
                        
                        dbs.getDBConfigFile().save();
                        File dbDir = new File(dbs.getConfig().getBaseDir(), operation.getDatabaseName());
                        
                        if (dbDir.exists()) {
                            FSUtils.delTree(dbDir);
                        }
                    }
                }
                
                return null;
            }
        });
        
        dbs.getTransactionManager().registerInMemoryProcessing(Operation.TYPE_COPY_DB, new InMemoryProcessing() {
            
            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {
                
                serialized.getInt(); // do not use, deprecated
                serialized.getInt(); // do not use, deprecated
                
                String sourceDB = serialized.getString();
                String destDB = serialized.getString();
                
                serialized.flip();
                
                return new Object[] { sourceDB, destDB };
            }
            
            @Override
            public OperationInternal convertToOperation(Object[] args) {
                return new BabuDBTransaction.BabuDBOperation(Operation.TYPE_COPY_DB, (String) args[0],
                        new Object[] { args[1] });
            }
            
            @Override
            public Object process(OperationInternal operation) throws BabuDBException {
                
                // parse args
                String destDB = (String) operation.getParams()[0];
                
                DatabaseInternal sDB = getDatabase(operation.getDatabaseName());
                
                int dbId;
                synchronized (getDBModificationLock()) {
                    synchronized (dbs.getCheckpointer()) {
                        if (dbsByName.containsKey(destDB)) {
                            throw new BabuDBException(ErrorCode.DB_EXISTS, "database '" + destDB + "' already exists");
                        }
                        dbId = nextDbId++;
                        // just "reserve" the name
                        dbsByName.put(destDB, null);
                        dbs.getDBConfigFile().save();
                    }
                }
                // materializing the snapshot takes some time, we should not
                // hold the
                // lock meanwhile!
                try {
                    sDB.proceedSnapshot(destDB);
                } catch (InterruptedException i) {
                    throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "Snapshot creation was interrupted.", i);
                }
                
                // create new DB and load from snapshot
                DatabaseInternal newDB = new DatabaseImpl(dbs, new LSMDatabase(destDB, dbId, dbs.getConfig()
                        .getBaseDir() + destDB + File.separatorChar, sDB.getLSMDB().getIndexCount(), true, sDB
                        .getComparators(), dbs.getConfig().getCompression(),
                        dbs.getConfig().getMaxNumRecordsPerBlock(), dbs.getConfig().getMaxBlockFileSize(), dbs
                                .getConfig().getDisableMMap(), dbs.getConfig().getMMapLimit()));
                
                // insert real database
                synchronized (dbModificationLock) {
                    dbsById.put(dbId, newDB);
                    dbsByName.put(destDB, newDB);
                    dbs.getDBConfigFile().save();
                }
                
                return null;
            }
        });
        
        dbs.getTransactionManager().registerInMemoryProcessing(Operation.TYPE_GROUP_INSERT, new InMemoryProcessing() {
            
            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {
                
                InsertRecordGroup irg = InsertRecordGroup.deserialize(serialized);
                serialized.flip();
                
                return new Object[] { irg, null };
            }
            
            @Override
            public OperationInternal convertToOperation(Object[] args) {
                return new BabuDBTransaction.BabuDBOperation(Operation.TYPE_GROUP_INSERT, (String) null,
                        new Object[] { args[0] });
            }
            
            @Override
            public Object process(OperationInternal operation) throws BabuDBException {
                
                Object[] args = operation.getParams();
                
                // parse args
                InsertRecordGroup irg = (InsertRecordGroup) args[0];
                LSMDatabase lsmDB = null;
                if (args.length > 1 && args[1] instanceof LSMDatabase) {
                    lsmDB = (LSMDatabase) args[1];
                }
                
                // complete the arguments
                if (lsmDB == null) {
                    
                    // set the DB ID, if unknown
                    if (irg.getDatabaseId() == InsertRecordGroup.DB_ID_UNKNOWN) {
                        irg.setDatabaseId(getDatabase(operation.getDatabaseName()).getLSMDB().getDatabaseId());
                    }
                    lsmDB = getDatabase(irg.getDatabaseId()).getLSMDB();
                    operation.updateParams(new Object[] { irg, lsmDB });
                }
                if (operation.getDatabaseName() == null) {
                    operation.updateDatabaseName(lsmDB.getDatabaseName());
                }
                
                int numIndices = lsmDB.getIndexCount();
                
                // check for user errors
                for (InsertRecord ir : irg.getInserts()) {
                    if ((ir.getIndexId() >= numIndices) || (ir.getIndexId() < 0)) {
                        
                        throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + ir.getIndexId()
                                + " does not exist");
                    }
                }
                
                // insert into the in-memory-tree
                for (InsertRecord ir : irg.getInserts()) {
                    LSMTree index = lsmDB.getIndex(ir.getIndexId());
                    
                    if (ir.getValue() != null) {
                        index.insert(ir.getKey(), ir.getValue());
                    } else {
                        index.delete(ir.getKey());
                    }
                }
                
                return null;
            }
        });
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#setNextDBId(int)
     */
    @Override
    public void setNextDBId(int id) {
        this.nextDbId = id;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getComparatorInstances
     * ()
     */
    @Override
    public Map<String, ByteRangeComparator> getComparatorInstances() {
        return compInstances;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#putDatabase(
     * org.xtreemfs.babudb.api.dev.DatabaseInternal)
     */
    @Override
    public void putDatabase(DatabaseInternal database) {
        dbsById.put(database.getLSMDB().getDatabaseId(), database);
        dbsByName.put(database.getName(), database);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getAllDatabaseIds()
     */
    @Override
    public Set<Integer> getAllDatabaseIds() {
        return new HashSet<Integer>(dbsById.keySet());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#removeDatabaseById
     * (int)
     */
    @Override
    public void removeDatabaseById(int id) {
        dbsByName.remove(dbsById.remove(id).getName());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getNextDBId()
     */
    @Override
    public int getNextDBId() {
        return nextDbId;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#createTransaction()
     */
    @Override
    public TransactionInternal createTransaction() {
        return new BabuDBTransaction();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#executeTransaction(
     * org.xtreemfs.babudb.api.dev.TransactionInternal)
     */
    @Override
    public synchronized void executeTransaction(TransactionInternal txn) throws BabuDBException {
        
        // acquire worker locks asynchronously if necessary
        if (dbs.getWorkerCount() > 0) {
            
            // maps the lockFuture of the workers affected by this txn
            Map<LSMDBWorker, BabuDBRequestResultImpl<AtomicBoolean>> workerLockFutureMap = new HashMap<LSMDBWorker, BabuDBRequestResultImpl<AtomicBoolean>>();
            
            // maps the lockFutures by the databases affected by this txn
            Map<String, DatabaseRequestResult<AtomicBoolean>> databaseLockFutureMap = new HashMap<String, DatabaseRequestResult<AtomicBoolean>>();
            
            for (String dbName : txn.databasesAffected()) {
                try {
                    // setup the lock-request if necessary
                    if (!databaseLockFutureMap.containsKey(dbName)) {
                        
                        LSMDBWorker worker = dbs.getWorker(getDatabase(dbName).getLSMDB().getDatabaseId());
                        
                        // retrieve the lockFuture of the worker if already
                        // available
                        BabuDBRequestResultImpl<AtomicBoolean> lockFuture = workerLockFutureMap.get(worker);
                        
                        // create a new lockFuture otherwise
                        if (lockFuture == null) {
                            lockFuture = new BabuDBRequestResultImpl<AtomicBoolean>(txn, dbs.getResponseManager());
                            worker.addRequest(new LSMDBRequest<AtomicBoolean>(lockFuture));
                            workerLockFutureMap.put(worker, lockFuture);
                        }
                        
                        databaseLockFutureMap.put(dbName, lockFuture);
                    }
                } catch (BabuDBException be) {
                    assert (be.getErrorCode() == ErrorCode.NO_SUCH_DB);
                    
                    /*
                     * affected database does not exist yet; exception will be
                     * ignored
                     */
                } catch (InterruptedException ie) {
                    throw new BabuDBException(ErrorCode.INTERRUPTED, ie.getMessage(), ie);
                }
            }
            
            txn.updateWorkerLocks(databaseLockFutureMap);
        }
        
        // execute the transaction
        BabuDBRequestResultImpl<Object> result = new BabuDBRequestResultImpl<Object>(dbs.getResponseManager());
        dbs.getTransactionManager().makePersistent(txn, result);
        result.get();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.DatabaseManager#executeTransaction(
     * org.xtreemfs.babudb.api.transaction.Transaction)
     */
    @Override
    public void executeTransaction(Transaction txn) throws BabuDBException {
        executeTransaction((TransactionInternal) txn);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.DatabaseManager#addTransactionListener(
     * org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void addTransactionListener(TransactionListener listener) {
        dbs.getTransactionManager().addTransactionListener(listener);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.DatabaseManager#removeTransactionListener(
     * org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void removeTransactionListener(TransactionListener listener) {
        dbs.getTransactionManager().removeTransactionListener(listener);
    }
    
    @Override
    public Object getRuntimeState(String property) {
        
        if (RUNTIME_STATE_DBCREATIONCOUNT.equals(property))
            return _dbCreationCount.get();
        if (RUNTIME_STATE_DBDELETIONCOUNT.equals(property))
            return _dbDeletionCount.get();
        
        return null;
    }
}
