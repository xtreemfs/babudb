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

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.dev.transaction.TransactionInternal;
import org.xtreemfs.babudb.api.dev.transaction.OperationInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.api.transaction.Operation;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * Standard implementation of a transaction. 
 * 
 * 
 * @author stenjan
 * @author flangner
 */
public class BabuDBTransaction extends TransactionInternal {
    private static final long serialVersionUID = 3772453774367730087L;
    
    private BabuDBException error = null;
    
    @Override
    public TransactionInternal createSnapshot(String databaseName, SnapshotConfig config) {
        return addOperation(new BabuDBOperation(TYPE_CREATE_SNAP, databaseName, 
                new Object[] { InsertRecordGroup.DB_ID_UNKNOWN, config }));
    }
    
    @Override
    public TransactionInternal deleteSnapshot(String databaseName, String snapshotName) {
        return addOperation(new BabuDBOperation(TYPE_DELETE_SNAP, databaseName, 
                new Object[] { snapshotName }));
    }
    
    @Override
    public TransactionInternal copyDatabase(String sourceName, String destinationName) {
        return addOperation(new BabuDBOperation(TYPE_COPY_DB, sourceName, 
                new Object[] { destinationName }));
    }
    
    @Override
    public TransactionInternal createDatabase(String databaseName, int numIndices) {
        return createDatabase(databaseName, numIndices, null);
    }
    
    @Override
    public TransactionInternal createDatabase(String databaseName, int numIndices, 
            ByteRangeComparator[] comparators) {
        
        return addOperation(new BabuDBOperation(TYPE_CREATE_DB, databaseName, 
                new Object[] {numIndices, comparators }));
    }
    
    @Override
    public TransactionInternal deleteDatabase(String databaseName) {
        return addOperation(new BabuDBOperation(TYPE_DELETE_DB, databaseName, null));
    }
    
    @Override
    public TransactionInternal deleteRecord(String databaseName, int indexId, byte[] key) {
        InsertRecordGroup irg = new InsertRecordGroup(-1);
        irg.addInsert(indexId, key, null);
        return insertRecordGroup(databaseName, irg);
    }
    
    @Override
    public TransactionInternal insertRecord(String databaseName, int indexId, byte[] key, 
            byte[] value) {
        
        InsertRecordGroup irg = new InsertRecordGroup(-1);
        irg.addInsert(indexId, key, value);
        return insertRecordGroup(databaseName, irg);
    }
    
    @Override
    public TransactionInternal insertRecordGroup(String databaseName, InsertRecordGroup irg) {
        return insertRecordGroup(databaseName, irg, null);
    }

    @Override
    public TransactionInternal insertRecordGroup(String databaseName, InsertRecordGroup irg, 
            LSMDatabase db) {
        
        return insertRecordGroup(databaseName, irg, db, null);
    }

    @Override
    public TransactionInternal insertRecordGroup(String databaseName, InsertRecordGroup irg, 
            LSMDatabase db, BabuDBRequestResultImpl<?> listener) {
        
        return addOperation(new BabuDBOperation(TYPE_GROUP_INSERT, databaseName, 
                new Object[] { irg, db, listener }));
    }
    
    @Override
    public List<Operation> getOperations() {
        return new LinkedList<Operation>(this);
    }
    
    @Override
    public TransactionInternal addOperation(OperationInternal op) {
        add(op);
        return this;
    }
    
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getName() + "\n");
        sb.append("operations:\n");
        for (Operation op : this)
            sb.append(op + "\n");
        
        return sb.toString();
    }
    
    @Override
    public int getSize() throws IOException {
        
        // determine the list of database names
        SortedSet<String> dbNames = new TreeSet<String>();
        for (Operation op : this) {
            dbNames.add(op.getDatabaseName());
        }
        
        // #dbNames + #ops
        int size = 2 * Integer.SIZE / 8;
        
        for (String dbName : dbNames) {
            size += Integer.SIZE / 8 + dbName.getBytes().length;
        }
        
        for (OperationInternal op : this) {
            size += op.getSize();
        }
        
        return size;
    }
    
    @Override
    public ReusableBuffer serialize(ReusableBuffer buffer) throws IOException {
        
        // determine the list of database names
        SortedSet<String> dbNames = new TreeSet<String>();
        for (Operation op : this) {
            dbNames.add(op.getDatabaseName());
        }
        
        // serialize the list of database names
        buffer.putInt(dbNames.size());
        for (String dbName : dbNames) {
            buffer.putInt(dbName.length());
            buffer.put(dbName.getBytes());
        }
        
        // serialize the list of parameters
        buffer.putInt(size());
        for (OperationInternal op : this) {
            op.serialize(dbNames, buffer);
        }
        
        return buffer;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionInternal#cutOfAt(int, 
     *          org.xtreemfs.babudb.api.exception.BabuDBException)
     */
    @Override
    public void cutOfAt(int position, BabuDBException reason) {
        
        assert (reason != null && error == null);
        error = reason;
        
        // position = 3
        // 0,1,2,3,4,5 (6)
        // 0,1,2,3,4   (5)
        // 0,1,2,3     (4)
        // 0,1,2       (3)
        while (size() > position) {
            removeLast();
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionInternal#getIrregularities()
     */
    @Override
    public BabuDBException getIrregularities() {
        return error;
    }
        
    public static class BabuDBOperation extends OperationInternal {
        
        private byte              type;
        
        private Object[]          params;
        
        private String            dbName;
        
        /**
         * @param type
         * @param dbName
         * @param params (may contain null values)
         */
        public BabuDBOperation(byte type, String dbName, Object[] params) {
            this.type = type;
            this.params = params;
            this.dbName = dbName;
        }
        
        @Override
        public Object[] getParams() {
            return params;
        }
        
        @Override
        public void updateParams(Object[] params) {
            this.params = params;
        }
        
        @Override
        public byte getType() {
            return type;
        }
        
        @Override
        public String getDatabaseName() {
            return dbName;
        }
        
        @Override
        public void updateDatabaseName(String dbName) {
            this.dbName = dbName;
        }
        
        @Override
        public int getSize() throws IOException {
            
            // initial size: type byte + number of parameters + db name index position
            int size = 1 + 2 * Integer.SIZE / 8;
            
            // add the size of all parameters
            if (params != null) {
                for (Object obj : params) {
                    
                    // exclude unserializable parameters
                    if (obj != null && 
                      !(obj instanceof LSMDatabase) && 
                      !(obj instanceof BabuDBRequestResultImpl<?>)) {
                        
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
                        else if (obj instanceof InsertRecordGroup) 
                            size += 1 + Integer.SIZE / 8 + ((InsertRecordGroup) obj).getSize();
                        
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
                }
            }
            
            return size;
        }
        
        public ReusableBuffer serialize(SortedSet<String> dbNameSet, ReusableBuffer buffer) 
                throws IOException {
            
            int size = 0, start = 0;
            assert ((size = getSize()) > -1);
            assert ((start = buffer.position()) > -1);
            
            buffer.put(type);
            
            // determine the index position in the database name set
            int dbNameIndex = InsertRecordGroup.DB_ID_UNKNOWN;
            Iterator<String> it = dbNameSet.iterator();
            for (int i = 0; it.hasNext(); i++) {
                String curr = it.next();
                if (curr.equals(dbName)) {
                    dbNameIndex = i;
                }
            }
            
            // serialize the database name index
            assert (dbNameIndex >= 0);
            buffer.putInt(dbNameIndex);
            
            // count the serializable parameters
            int count = 0;
            if (params != null) {
                for (Object obj : params) {
                    
                    // exclude unserializable parameters
                    if (obj != null && 
                      !(obj instanceof LSMDatabase) && 
                      !(obj instanceof BabuDBRequestResultImpl<?>)) {
                        count++;
                    }
                }
            }
            
            buffer.putInt(count);
            
            // serialize the list of parameters
            if (params != null) {
                for (Object obj : params) {
                    
                    // exclude unserializable parameters
                    if (obj != null && 
                      !(obj instanceof LSMDatabase) && 
                      !(obj instanceof BabuDBRequestResultImpl<?>)) {
                        
                        serializeParam(buffer, obj);
                    }
                }
            }
            
            assert ((buffer.position() - start) == size) : 
                "Operation " + type + " was not serialized successfully!";
            
            return buffer;
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
            } else if (obj instanceof InsertRecordGroup) {
                
                InsertRecordGroup irg = (InsertRecordGroup) obj;
                
                int size = irg.getSize();
                ReusableBuffer buf = BufferPool.allocate(size);
                irg.serialize(buf);
                buf.flip();
                
                buffer.put(FIELD_TYPE_GROUP);
                buffer.putInt(size);
                buffer.put(buf);
                BufferPool.free(buf);
                
            // perform a Java serialization for any other types, e.g.
            // ByteRangeComparators
            } else {
                
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
        
        public static Object deserializeParam(ReusableBuffer buffer) throws IOException {
            
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
            case FIELD_TYPE_GROUP: {
                
                int length = buffer.getInt();
                ReusableBuffer view = null;
                try {
                    int bufferPos = buffer.position();
                    int pos = bufferPos + length;
                    
                    // prepare view buffer
                    view = buffer.createViewBuffer();
                    view.position(bufferPos);
                    view.limit(pos);
                    
                    // reset buffer position
                    buffer.position(pos);
                    
                    return InsertRecordGroup.deserialize(view);
                } finally {
                    if (view != null) BufferPool.free(view);
                }
            }
            default:
                throw new IOException("invalid field type:" + fieldType);
            }
        }
    }
}
