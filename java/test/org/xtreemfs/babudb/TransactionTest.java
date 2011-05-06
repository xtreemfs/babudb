/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.xtreemfs.babudb;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.junit.Test;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.api.transaction.Operation;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.lsmdb.BabuDBTransaction;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * 
 * @author bjko
 */
public class TransactionTest extends TestCase {
    
    public TransactionTest() {
        Logging.start(Logging.LEVEL_DEBUG);
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
        assertEquals(1, txn.getOperations().get(0).getParams().length);
        
        assertEquals(Operation.TYPE_CREATE_DB, txn.getOperations().get(1).getType());
        assertEquals("db1", txn.getOperations().get(1).getDatabaseName());
        assertEquals(2, txn.getOperations().get(1).getParams().length);
        
        assertEquals(Operation.TYPE_INSERT_KEY, txn.getOperations().get(2).getType());
        assertEquals("db1", txn.getOperations().get(2).getDatabaseName());
        assertEquals(3, txn.getOperations().get(2).getParams().length);
        
        assertEquals(Operation.TYPE_DELETE_KEY, txn.getOperations().get(3).getType());
        assertEquals("db2", txn.getOperations().get(3).getDatabaseName());
        assertEquals(2, txn.getOperations().get(3).getParams().length);
        
        assertEquals(Operation.TYPE_DELETE_DB, txn.getOperations().get(4).getType());
        assertEquals("db1", txn.getOperations().get(4).getDatabaseName());
        assertEquals(0, txn.getOperations().get(4).getParams().length);
        
        assertEquals(Operation.TYPE_INSERT_KEY, txn.getOperations().get(5).getType());
        assertEquals("new-database", txn.getOperations().get(5).getDatabaseName());
        assertEquals(3, txn.getOperations().get(5).getParams().length);
        
        // serialize transaction to buffer
        int size = txn.getSize();
        ReusableBuffer buf = BufferPool.allocate(size);
        txn.serialize(buf);
        assertEquals(buf.position(), size);
        
        // deserialize transaction from buffer
        buf.position(0);
        BabuDBTransaction txn2 = BabuDBTransaction.deserialize(buf);
        assertEquals(buf.position(), size);
        
        // compare original transaction with deserialized transaction
        assertEquals(txn.getSize(), txn2.getSize());
        assertEquals(txn.getOperations().size(), txn2.getOperations().size());
        for (int i = 0; i < txn.getOperations().size(); i++) {
            
            Operation op1 = txn.getOperations().get(i);
            Operation op2 = txn2.getOperations().get(i);
            
            assertEquals(op1.getType(), op2.getType());
            assertEquals(op1.getParams().length, op2.getParams().length);
            
            for (int j = 0; j < op1.getParams().length; j++) {
                
                Object p1 = op1.getParams()[j];
                Object p2 = op2.getParams()[j];
                
                if (p1 instanceof Number || p1 instanceof String)
                    assertEquals(p1, p2);
                
                else if (p1 instanceof byte[]) {
                    
                    byte[] b1 = (byte[]) p1;
                    byte[] b2 = (byte[]) p2;
                    
                    assertEquals(b1.length, b2.length);
                    for(int k = 0; k < b1.length; k++)
                        assertEquals(b1[k], b2[k]);
                    
                }
                
            }
            
        }
        
    }
    
    public static void main(String[] args) {
        TestRunner.run(TransactionTest.class);
    }
    
}