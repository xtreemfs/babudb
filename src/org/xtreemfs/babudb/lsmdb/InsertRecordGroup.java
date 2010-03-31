/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.lsmdb;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * 
 * @author bjko
 */
public class InsertRecordGroup {
    
    private List<InsertRecord> records;
    
    private int                databaseId;
    
    public InsertRecordGroup(int databaseId) {
        records = new LinkedList();
        this.databaseId = databaseId;
    }
    
    public void addInsert(int indexId, byte[] key, byte[] value) {
        records.add(new InsertRecord(indexId, key, value));
    }
    
    public List<InsertRecord> getInserts() {
        return records;
    }
    
    public int getDatabaseId() {
        return databaseId;
    }
    
    private void addInsertRecord(InsertRecord rec) {
        assert (rec != null);
        records.add(rec);
    }
    
    public int getSize() {
        int size = Integer.SIZE / 8;
        for (InsertRecord ir : records)
            size += ir.getSize();
        return size;
    }
    
    public void serialize(ReusableBuffer buffer) {
        buffer.putInt(databaseId);
        for (InsertRecord ir : records)
            ir.serialize(buffer);
    }
    
    public static InsertRecordGroup deserialize(ReusableBuffer buffer) {
        int dbId = buffer.getInt();
        InsertRecordGroup ai = new InsertRecordGroup(dbId);
        while (buffer.hasRemaining()) {
            InsertRecord ir = InsertRecord.deserialize(buffer);
            ai.addInsertRecord(ir);
        }
        return ai;
    }
    
    public static class InsertRecord {
        private final byte   indexId;
        
        private final byte[] key;
        
        private final byte[] value;
        
        public InsertRecord(int indexId, byte[] key, byte[] value) {
            this.indexId = (byte) indexId;
            this.key = key;
            this.value = value;
        }
        
        public int getIndexId() {
            return 0x00FF & indexId;
        }
        
        public byte[] getKey() {
            return key;
        }
        
        public byte[] getValue() {
            return value;
        }
        
        public int getSize() {
            if (value != null)
                return Byte.SIZE / 8 + Integer.SIZE / 8 * 2 + key.length + value.length;
            else
                return Byte.SIZE / 8 + Integer.SIZE / 8 * 2 + key.length;
        }
        
        public void serialize(ReusableBuffer buffer) {
            buffer.put(indexId);
            buffer.putInt(key.length);
            buffer.put(key);
            if (value != null) {
                buffer.putInt(value.length);
                buffer.put(value);
            } else {
                buffer.putInt(0);
            }
        }
        
        public static InsertRecord deserialize(ReusableBuffer buffer) {
            byte tmp = buffer.get();
            int indexId = 0x00FF & tmp;
            
            int size = buffer.getInt();
            byte[] key = new byte[size];
            buffer.get(key);
            
            size = buffer.getInt();
            byte[] value = null;
            if (size > 0) {
                value = new byte[size];
                buffer.get(value);
            }
            
            return new InsertRecord(indexId, key, value);
        }
        
        public String toString() {
            
            StringBuilder sb = new StringBuilder();
            sb.append("index: " + indexId);
            sb.append(", key: " + (key == null ? null : Arrays.toString(key)));
            sb.append(", value: " + (value == null ? null : Arrays.toString(value)));
            
            return sb.toString();
        }
        
    }
    
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getName() + "\n");
        sb.append("database: " + databaseId + "\n");
        for (InsertRecord record : records)
            sb.append(record + "\n");
        
        return sb.toString();
    }
}
