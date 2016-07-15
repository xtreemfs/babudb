/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.xtreemfs.babudb;

import java.io.File;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.dev.transaction.TransactionInternal;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.api.transaction.Operation;
import org.xtreemfs.babudb.api.transaction.Transaction;
import org.xtreemfs.babudb.api.transaction.TransactionListener;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.BabuDBTransaction;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.FSUtils;

/**
 * 
 * @author stenjan
 */
public class TransactionTest extends TestCase {
    
    public static final String  baseDir          = "/tmp/lsmdb-test/";
    
    public static final int     LOG_LEVEL        = Logging.LEVEL_ERROR;
    
    public static final boolean MMAP             = false;
    
    public static final boolean COMPRESSION      = false;
    
    private static final int    maxNumRecs       = 16;
    
    private static final int    maxBlockFileSize = 1024 * 1024 * 512;
    
    private BabuDB              database;
    
    public TransactionTest() {
        Logging.start(LOG_LEVEL);
    }
    
    @Before
    public void setUp() throws Exception {
        
        FSUtils.delTree(new File(baseDir));
        
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 3, 0, 0, SyncMode.ASYNC, 0,
            0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, LOG_LEVEL));
        
        System.out.println("=== " + getName() + " ===");
    }
    
    @After
    public void tearDown() throws Exception {
        database.shutdown();
    }
    
    @Test
    public void testTransactionSerialization() throws Exception {
        
        // create and initialize a transaction
        BabuDBTransaction txn = new BabuDBTransaction();
        txn.createDatabase("db1", 5);
        txn.createDatabase("db1", 1, new ByteRangeComparator[] { new DefaultByteRangeComparator() });
        txn.insertRecord("db1", 3, "hello".getBytes(), "world".getBytes());
        txn.deleteRecord("db2", 1, "blub".getBytes());
        txn.deleteDatabase("db1");
        txn.insertRecord("new-database", 3, "x".getBytes(), "".getBytes());
        
        // check transaction
        assertEquals(6, txn.getOperations().size());
        
        assertEquals(Operation.TYPE_CREATE_DB, txn.getOperations().get(0).getType());
        assertEquals("db1", txn.getOperations().get(0).getDatabaseName());
        assertEquals(2, txn.getOperations().get(0).getParams().length);
        
        assertEquals(Operation.TYPE_CREATE_DB, txn.getOperations().get(1).getType());
        assertEquals("db1", txn.getOperations().get(1).getDatabaseName());
        assertEquals(2, txn.getOperations().get(1).getParams().length);
        
        assertEquals(Operation.TYPE_GROUP_INSERT, txn.getOperations().get(2).getType());
        assertEquals("db1", txn.getOperations().get(2).getDatabaseName());
        assertEquals(2, txn.getOperations().get(2).getParams().length);
        
        assertEquals(Operation.TYPE_GROUP_INSERT, txn.getOperations().get(3).getType());
        assertEquals("db2", txn.getOperations().get(3).getDatabaseName());
        assertEquals(2, txn.getOperations().get(3).getParams().length);
        
        assertEquals(Operation.TYPE_DELETE_DB, txn.getOperations().get(4).getType());
        assertEquals("db1", txn.getOperations().get(4).getDatabaseName());
        assertNull(txn.getOperations().get(4).getParams());
        
        assertEquals(Operation.TYPE_GROUP_INSERT, txn.getOperations().get(5).getType());
        assertEquals("new-database", txn.getOperations().get(5).getDatabaseName());
        assertEquals(2, txn.getOperations().get(5).getParams().length);
        
        // test of the aggregate function
        byte aggregate = txn.aggregateOperationTypes();
        assertTrue(TransactionInternal.containsOperationType(aggregate, Operation.TYPE_CREATE_DB));
        assertTrue(TransactionInternal.containsOperationType(aggregate, Operation.TYPE_CREATE_DB));
        assertTrue(TransactionInternal.containsOperationType(aggregate, Operation.TYPE_GROUP_INSERT));
        assertTrue(TransactionInternal.containsOperationType(aggregate, Operation.TYPE_DELETE_DB));
        assertTrue(TransactionInternal.containsOperationType(aggregate, Operation.TYPE_DELETE_DB));
        assertFalse(TransactionInternal.containsOperationType(aggregate, Operation.TYPE_CREATE_SNAP));
        
        // serialize transaction to buffer
        int size = txn.getSize();
        ReusableBuffer buf = BufferPool.allocate(size);
        txn.serialize(buf);
        assertEquals(buf.position(), size);
        
        // deserialize transaction from buffer
        buf.position(0);
        TransactionInternal txn2 = TransactionInternal.deserialize(buf);
        assertEquals(buf.position(), size);
        
        // compare original transaction with deserialized transaction
        assertEquals(txn.getSize(), txn2.getSize());
        assertEquals(txn.getOperations().size(), txn2.getOperations().size());
        for (int i = 0; i < txn.getOperations().size(); i++) {
            
            Operation op1 = txn.getOperations().get(i);
            Operation op2 = txn2.getOperations().get(i);
            
            // count legal parameters
            int count = 0;
            if (op1.getParams() != null) {
                for (Object obj : op1.getParams()) {
                    if (obj != null && !(obj instanceof LSMDatabase)
                        && !(obj instanceof BabuDBRequestResultImpl<?>)) {
                        count++;
                    }
                }
            }
            
            assertEquals(op1.getType(), op2.getType());
            if (count > 0) {
                assertEquals(count, op2.getParams().length);
            } else {
                assertNull(op2.getParams());
            }
            if (op2.getParams() != null) {
                for (int j = 0; j < op2.getParams().length; j++) {
                    
                    Object p1 = op1.getParams()[j];
                    Object p2 = op2.getParams()[j];
                    
                    if (p1 instanceof Number || p1 instanceof String)
                        assertEquals(p1, p2);
                    
                    else if (p1 instanceof byte[]) {
                        
                        byte[] b1 = (byte[]) p1;
                        byte[] b2 = (byte[]) p2;
                        
                        assertEquals(b1.length, b2.length);
                        for (int k = 0; k < b1.length; k++)
                            assertEquals(b1[k], b2[k]);
                    }
                    
                }
            }
        }
    }
    
    @Test
    public void testTransactionExecution() throws Exception {
        
        DatabaseManager dbMan = database.getDatabaseManager();
        
        // create and execute a transaction
        Transaction txn = dbMan.createTransaction();
        txn.createDatabase("test", 3);
        txn.insertRecord("test", 0, "hello".getBytes(), "world".getBytes());
        txn.insertRecord("test", 1, "key".getBytes(), "value".getBytes());
        dbMan.executeTransaction(txn);
        
        // check if the database is there
        Database db = dbMan.getDatabase("test");
        assertNotNull(db);
        assertEquals("test", db.getName());
        assertEquals(3, db.getComparators().length);
        
        // check if the records are there
        byte[] value = db.lookup(0, "hello".getBytes(), null).get();
        assertEquals("world", new String(value));
        
        value = db.lookup(1, "key".getBytes(), null).get();
        assertEquals("value", new String(value));
        
        // create and execute a second transaction
        txn = dbMan.createTransaction();
        txn.createDatabase("test2", 1);
        txn.deleteRecord("test", 0, "hello".getBytes());
        dbMan.executeTransaction(txn);
        
        // check if the both databases are there
        db = dbMan.getDatabase("test");
        assertNotNull(db);
        assertEquals("test", db.getName());
        assertEquals(3, db.getComparators().length);
        
        db = dbMan.getDatabase("test2");
        assertNotNull(db);
        assertEquals("test2", db.getName());
        assertEquals(1, db.getComparators().length);
        
        // check if the key got deleted
        value = db.lookup(0, "hello".getBytes(), null).get();
        assertNull(value);
        
        // create and execute a third transaction
        txn = dbMan.createTransaction();
        txn.deleteDatabase("test");
        txn.deleteDatabase("test2");
        dbMan.executeTransaction(txn);
        
        // check if all databases were deleted
        assertEquals(0, dbMan.getDatabases().size());
        
        // create and execute an empty transaction
        txn = dbMan.createTransaction();
        dbMan.executeTransaction(txn);
        
    }
    
    @Test
    public void testTransactionPersistence() throws Exception {
        
        DatabaseManager dbMan = database.getDatabaseManager();
        
        // create and execute a transaction
        Transaction txn = dbMan.createTransaction();
        txn.createDatabase("test", 2);
        txn.insertRecord("test", 0, "hello".getBytes(), "world".getBytes());
        txn.insertRecord("test", 1, "key".getBytes(), "value".getBytes());
        dbMan.executeTransaction(txn);
        
        // shutdown and restart the database
        database.shutdown();
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0, SyncMode.ASYNC, 0,
            0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, LOG_LEVEL));
        
        // retrieve the dbMan of the restarted BabuDB
        dbMan = database.getDatabaseManager();
        
        // check if the database is there
        Database db = dbMan.getDatabase("test");
        assertNotNull(db);
        assertEquals("test", db.getName());
        assertEquals(2, db.getComparators().length);
        
        // check if the records are there
        byte[] value = db.lookup(0, "hello".getBytes(), null).get();
        assertEquals("world", new String(value));
        
        value = db.lookup(1, "key".getBytes(), null).get();
        assertEquals("value", new String(value));
        
        // enforce a checkpoint
        database.getCheckpointer().checkpoint();
        
        // shutdown and restart the database
        database.shutdown();
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 0, 0, 0, SyncMode.ASYNC, 0,
            0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, LOG_LEVEL));
        
        // retrieve the dbMan of the restarted BabuDB
        dbMan = database.getDatabaseManager();
        
        // check if the database is there
        db = dbMan.getDatabase("test");
        assertNotNull(db);
        assertEquals("test", db.getName());
        assertEquals(2, db.getComparators().length);
        
        // check if the records are there
        value = db.lookup(0, "hello".getBytes(), null).get();
        assertEquals("world", new String(value));
        
        value = db.lookup(1, "key".getBytes(), null).get();
        assertEquals("value", new String(value));
        
    }
    
    @Test
    public void testTransactionWorkerInteraction() throws Exception {
        
        Database db0 = database.getDatabaseManager().createDatabase("workerTest0", 1);
        Database db1 = database.getDatabaseManager().createDatabase("workerTest1", 2);
        Database db2 = database.getDatabaseManager().createDatabase("workerTest2", 3);
        
        // make some inserts to fill up the worker queue
        for (int i = 0; i < 100; i++) {
            DatabaseInsertGroup ir = db0.createInsertGroup();
            ir.addInsert(0, (i + "").getBytes(), "bla".getBytes());
            db0.insert(ir, null);
            
            ir = db1.createInsertGroup();
            ir.addInsert(0, (i + "").getBytes(), "bla".getBytes());
            ir.addInsert(1, (i + "").getBytes(), "blubb".getBytes());
            db1.insert(ir, null);
            
            ir = db2.createInsertGroup();
            ir.addInsert(0, (i + "").getBytes(), "bla".getBytes());
            ir.addInsert(1, (i + "").getBytes(), "blubb".getBytes());
            ir.addInsert(2, (i + "").getBytes(), "yagga".getBytes());
            db2.insert(ir, null);
        }
        
        // make a transaction to delete the last 3 keys of two of the databases
        // an update anomaly will be visible, if something went wrong
        Transaction txn = database.getDatabaseManager().createTransaction();
        txn.deleteRecord(db0.getName(), 0, "99".getBytes());
        txn.deleteRecord(db0.getName(), 0, "98".getBytes());
        txn.deleteRecord(db0.getName(), 0, "97".getBytes());
        txn.deleteRecord(db2.getName(), 2, "99".getBytes());
        txn.deleteRecord(db2.getName(), 2, "98".getBytes());
        txn.deleteRecord(db2.getName(), 2, "97".getBytes());
        database.getDatabaseManager().executeTransaction(txn);
        assertNull(db0.lookup(0, "99".getBytes(), null).get());
        assertNull(db0.lookup(0, "98".getBytes(), null).get());
        assertNull(db0.lookup(0, "97".getBytes(), null).get());
        assertNull(db2.lookup(2, "99".getBytes(), null).get());
        assertNull(db2.lookup(2, "98".getBytes(), null).get());
        assertNull(db2.lookup(2, "97".getBytes(), null).get());
    }
    
    @Test
    public void testTransactionListeners() throws Exception {
        
        final AtomicInteger count = new AtomicInteger(0);
        
        DatabaseManager dbMan = database.getDatabaseManager();
        
        // add a transaction listener
        TransactionListener l0 = new TransactionListener() {
            public void transactionPerformed(Transaction txn) {
                count.incrementAndGet();
            }
        };
        dbMan.addTransactionListener(l0);
        
        // create and execute a transaction
        Transaction txn = dbMan.createTransaction();
        txn.createDatabase("test", 1);
        txn.insertRecord("test", 0, "hello".getBytes(), "world".getBytes());
        txn.insertRecord("test", 0, "key".getBytes(), "value".getBytes());
        dbMan.executeTransaction(txn);
        
        assertEquals(1, count.get());
        
        // create and execute another transaction (which is empty)
        txn = dbMan.createTransaction();
        
        // add a listener before executing the transaction and wait for the
        // notification
        TransactionListener l1 = new TransactionListener() {
            public void transactionPerformed(Transaction txn) {
                count.incrementAndGet();
            }
        };
        dbMan.addTransactionListener(l1);
        dbMan.executeTransaction(txn);
        
        assertEquals(3, count.get());
        
    }
    
    @Test
    public void testTransactionListenerDBAccess() throws Throwable {
        
        final int numTxns = 10;
        final int numKVPairs = 20;
        final int numIndices = 5;
        
        final DatabaseManager dbMan = database.getDatabaseManager();
        final List<Throwable> errors = new LinkedList<Throwable>();
        
        // add a transaction listener
        TransactionListener l = new TransactionListener() {
            public void transactionPerformed(Transaction txn) {
                try {
                    // check if the database has been properly created and
                    // populated
                    checkDBContent(dbMan, txn.getOperations().get(0).getDatabaseName(), numIndices,
                        numKVPairs);
                } catch (Throwable e) {
                    errors.add(e);
                }
            }
        };
        dbMan.addTransactionListener(l);
        
        // create and execute multiple transactions: each creates a new database
        // and populates multiple indices
        for (int i = 0; i < numTxns; i++) {
            
            String dbName = i + "";
            
            Transaction txn = dbMan.createTransaction();
            txn.createDatabase(dbName, numIndices);
            for (int j = 0; j < numKVPairs; j++) {
                String kv = intToString(j, ("" + numKVPairs).length());
                txn.insertRecord(dbName, j % numIndices, kv.getBytes(), kv.getBytes());
            }
            dbMan.executeTransaction(txn);
        }
        
        if (errors.size() > 0)
            throw errors.get(0);
        
    }
    
    private void checkDBContent(DatabaseManager dbMan, String dbName, int numIndices, int numKVPairs)
        throws Throwable {
        
        Database db = dbMan.getDatabase(dbName);
        
        for (int j = 0; j < numIndices; j++) {
            
            ResultSet<byte[], byte[]> resultSet = db.prefixLookup(j, new byte[0], null).get();
            for (int i = j; i < numKVPairs; i += numIndices) {
                
                assertTrue(resultSet.hasNext());
                String kv = intToString(i, ("" + numKVPairs).length());
                
                Entry<byte[], byte[]> next = resultSet.next();
                assertEquals(kv, new String(next.getKey()));
                assertEquals(kv, new String(next.getValue()));
            }
            assertFalse(resultSet.hasNext());
            
            resultSet.free();
        }
    }
    
    private static String intToString(int num, int numDigits) {
        
        String pattern = "";
        for (int i = 0; i < numDigits; i++)
            pattern += "0";
        
        DecimalFormat df = new DecimalFormat(pattern);
        return df.format(num);
    }
    
    public static void main(String[] args) {
        TestRunner.run(TransactionTest.class);
    }
    
}