/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.lsmdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.api.transaction.Operation;
import org.xtreemfs.babudb.api.transaction.Transaction;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * Standard implementation of a transaction.
 * 
 * 
 * @author stenjan
 */
public class BabuDBTransaction implements Transaction {
    
    private List<Operation> ops;
    
    public BabuDBTransaction() {
        ops = new LinkedList<Operation>();
    }
    
    @Override
    public void deleteRecord(String databaseName, int indexId, byte[] key) {
        ops.add(new BabuDBOperation(Operation.TYPE_DELETE_KEY, databaseName, indexId, key));
    }
    
    @Override
    public void insertRecord(String databaseName, int indexId, byte[] key, byte[] value) {
        ops.add(new BabuDBOperation(Operation.TYPE_INSERT_KEY, databaseName, indexId, key, value));
    }
    
    @Override
    public void createDatabase(String databaseName, int numIndices) {
        ops.add(new BabuDBOperation(Operation.TYPE_CREATE_DB, databaseName, numIndices));
    }
    
    @Override
    public void createDatabase(String databaseName, int numIndices, ByteRangeComparator[] comparators) {
        ops.add(new BabuDBOperation(Operation.TYPE_CREATE_DB, databaseName, numIndices, comparators));
    }
    
    @Override
    public void deleteDatabase(String databaseName) {
        ops.add(new BabuDBOperation(Operation.TYPE_DELETE_DB, databaseName));
    }
    
    @Override
    public List<Operation> getOperations() {
        return ops;
    }
    
    public void addOperation(BabuDBOperation op) {
        ops.add(op);
    }
    
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getName() + "\n");
        sb.append("operations:\n");
        for (Operation op : ops)
            sb.append(op + "\n");
        
        return sb.toString();
    }
    
    public int getSize() throws IOException {
        
        // determine the list of datbase names
        SortedSet<String> dbNames = new TreeSet<String>();
        for (Operation op : ops)
            dbNames.add(op.getDatabaseName());
        
        // #dbNames + #ops
        int size = 2 * Integer.SIZE / 8;
        
        for (String dbName : dbNames)
            size += Integer.SIZE / 8 + dbName.getBytes().length;
        
        for (Operation op : ops)
            size += ((BabuDBOperation) op).getSize();
        
        return size;
    }
    
    /**
     * Serializes the transaction to a buffer.
     * <p>
     * The following format is used:
     * <ol>
     * <li> length of the list of all database names (4 bytes)
     * <li> all database names (4 bytes for the length + #chars)
     * <li> length of the list of operations (4 bytes)
     * <li> all operations (variable size)
     * </ol>
     * </p>
     * 
     * @param buffer the buffer
     * @throws IOException
     */
    public void serialize(ReusableBuffer buffer) throws IOException {
        
        // determine the list of datbase names
        SortedSet<String> dbNames = new TreeSet<String>();
        for (Operation op : ops)
            dbNames.add(op.getDatabaseName());
        
        // serialize the list of database names
        buffer.putInt(dbNames.size());
        for (String dbName : dbNames) {
            buffer.putInt(dbName.length());
            buffer.put(dbName.getBytes());
        }
        
        // serialize the list of parameters
        buffer.putInt(ops.size());
        for (Operation op : ops)
            ((BabuDBOperation) op).serialize(dbNames, buffer);
    }
    
    public static BabuDBTransaction deserialize(ReusableBuffer buffer) throws IOException {
        
        BabuDBTransaction txn = new BabuDBTransaction();
        
        // deserialize the list of database names
        int length = buffer.getInt();
        String[] dbNames = new String[length];
        for (int i = 0; i < length; i++) {
            byte[] bytes = new byte[buffer.getInt()];
            buffer.get(bytes);
            dbNames[i] = new String(bytes);
        }
        
        // deserialize the list of operations
        length = buffer.getInt();
        for (int i = 0; i < length; i++) {
            BabuDBOperation op = BabuDBOperation.deserialize(dbNames, buffer);
            txn.addOperation(op);
        }
        return txn;
    }
    
    public static class BabuDBOperation implements Operation {
        
        private static final byte FIELD_TYPE_INTEGER    = 0;
        
        private static final byte FIELD_TYPE_SHORT      = 1;
        
        private static final byte FIELD_TYPE_LONG       = 2;
        
        private static final byte FIELD_TYPE_BOOLEAN    = 3;
        
        private static final byte FIELD_TYPE_BYTE       = 4;
        
        private static final byte FIELD_TYPE_BYTE_ARRAY = 5;
        
        private static final byte FIELD_TYPE_STRING     = 6;
        
        private static final byte FIELD_TYPE_OBJECT     = 7;
        
        private byte              type;
        
        private Object[]          params;
        
        private String            dbName;
        
        public BabuDBOperation(byte type, String dbName, Object... params) {
            this.type = type;
            this.params = params;
            this.dbName = dbName;
        }
        
        @Override
        public Object[] getParams() {
            return params;
        }
        
        @Override
        public byte getType() {
            return type;
        }
        
        @Override
        public String getDatabaseName() {
            return dbName;
        }
        
        public int getSize() throws IOException {
            
            // initial size: type byte + number of parameters + db name index
            // position
            int size = 1 + 2 * Integer.SIZE / 8;
            
            // add the size of all parameters
            for (Object obj : params) {
                
                // perform a straight-forward serialization of primitive types
                // and strings
                if (obj instanceof String)
                    size += 1 + Integer.SIZE / 8 + ((String) obj).getBytes().length;
                else if (obj instanceof Integer)
                    size += 1 + Integer.SIZE / 8;
                else if (obj instanceof Long)
                    size += 1 + Long.SIZE / 8;
                else if (obj instanceof Short)
                    size += 1 + Short.SIZE / 8;
                else if (obj instanceof Byte)
                    size += 1 + Byte.SIZE / 8;
                else if (obj instanceof byte[])
                    size += 1 + Integer.SIZE / 8 + ((byte[]) obj).length;
                else if (obj instanceof Boolean)
                    size += 1 + 1;
                
                // perform a Java serialization for any other types, e.g.
                // ByteRangeComparators
                else {
                    ByteArrayOutputStream bout = new ByteArrayOutputStream();
                    ObjectOutputStream out = new ObjectOutputStream(bout);
                    out.writeObject(obj);
                    
                    // length + content
                    size += 1 + Integer.SIZE / 8 + bout.toByteArray().length;
                    out.close();
                }
            }
            
            return size;
        }
        
        public void serialize(SortedSet<String> dbNameSet, ReusableBuffer buffer) throws IOException {
            
            buffer.put(type);
            
            // determine the index position in the database name set
            int dbNameIndex = -1;
            Iterator<String> it = dbNameSet.iterator();
            for (int i = 0; it.hasNext(); i++) {
                String curr = it.next();
                if (curr.equals(dbName))
                    dbNameIndex = i;
            }
            
            // serialize the database name index
            assert (dbNameIndex >= 0);
            buffer.putInt(dbNameIndex);
            
            // serialize the list of parameters
            buffer.putInt(params.length);
            for (Object obj : params)
                serializeParam(buffer, obj);
        }
        
        public static BabuDBOperation deserialize(String[] dbNameList, ReusableBuffer buffer)
            throws IOException {
            
            // deserialize type of the operation
            byte type = buffer.get();
            
            // deserialize the database name
            int dbNameIndex = buffer.getInt();
            String dbName = dbNameList[dbNameIndex];
            
            // deserialize number of parameters
            int length = buffer.getInt();
            LinkedList<Object> params = new LinkedList<Object>();
            for (int i = 0; i < length; i++)
                params.add(deserializeParam(buffer));
            
            return new BabuDBOperation(type, dbName, params.toArray());
        }
        
        public String toString() {
            
            StringBuilder sb = new StringBuilder();
            sb.append("opcode: " + type + "\n");
            sb.append("params: " + (params == null ? null : Arrays.toString(params)));
            
            return sb.toString();
        }
        
        private static void serializeParam(ReusableBuffer buffer, Object obj) throws IOException {
            
            // perform a straight-forward serialization of primitive types
            // and strings
            if (obj instanceof String) {
                
                String string = (String) obj;
                
                buffer.put(FIELD_TYPE_STRING);
                buffer.putInt(string.getBytes().length);
                buffer.put(string.getBytes());
                
            } else if (obj instanceof byte[]) {
                
                byte[] bytes = (byte[]) obj;
                
                buffer.put(FIELD_TYPE_BYTE_ARRAY);
                buffer.putInt(bytes.length);
                buffer.put(bytes);
                
            } else if (obj instanceof Integer) {
                
                Integer integer = (Integer) obj;
                
                buffer.put(FIELD_TYPE_INTEGER);
                buffer.putInt(integer);
                
            } else if (obj instanceof Long) {
                
                Long longInt = (Long) obj;
                
                buffer.put(FIELD_TYPE_LONG);
                buffer.putLong(longInt);
                
            } else if (obj instanceof Short) {
                
                Short shortInt = (Short) obj;
                
                buffer.put(FIELD_TYPE_SHORT);
                buffer.putShort(shortInt);
                
            } else if (obj instanceof Boolean) {
                
                Boolean bool = (Boolean) obj;
                
                buffer.put(FIELD_TYPE_BOOLEAN);
                buffer.putBoolean(bool);
                
            } else if (obj instanceof Byte) {
                
                Byte byteObj = (Byte) obj;
                
                buffer.put(FIELD_TYPE_BYTE);
                buffer.put(byteObj);
            }

            // perform a Java serialization for any other types, e.g.
            // ByteRangeComparators
            else {
                
                buffer.put(FIELD_TYPE_OBJECT);
                
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bout);
                out.writeObject(obj);
                out.close();
                
                byte[] bytes = bout.toByteArray();
                buffer.putInt(bytes.length);
                buffer.put(bytes);
                
                bout.close();
            }
            
        }
        
        private static Object deserializeParam(ReusableBuffer buffer) throws IOException {
            
            byte fieldType = buffer.get();
            
            // perform a straight-forward deserialization of primitive types
            // and strings
            switch (fieldType) {
            case FIELD_TYPE_INTEGER:
                return buffer.getInt();
            case FIELD_TYPE_SHORT:
                return buffer.getShort();
            case FIELD_TYPE_LONG:
                return buffer.getLong();
            case FIELD_TYPE_BOOLEAN:
                return buffer.getBoolean();
            case FIELD_TYPE_BYTE:
                return buffer.get();
            case FIELD_TYPE_BYTE_ARRAY: {
                int length = buffer.getInt();
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                return bytes;
            }
            case FIELD_TYPE_STRING: {
                
                byte[] bytes = new byte[buffer.getInt()];
                buffer.get(bytes);
                
                return new String(bytes);
                
            }
            case FIELD_TYPE_OBJECT: {
                int length = buffer.getInt();
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                
                ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
                Object obj;
                try {
                    obj = in.readObject();
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
                in.close();
                
                return obj;
            }
            default:
                throw new IOException("invalid field type:" + fieldType);
            }
            
        }
        
    }
    
}
