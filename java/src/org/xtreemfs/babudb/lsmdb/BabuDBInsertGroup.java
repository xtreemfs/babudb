/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.lsmdb;


/**
 *
 * @author bjko
 */
public class BabuDBInsertGroup {

    private final InsertRecordGroup rec;
    
    private boolean freed;
    
    BabuDBInsertGroup(LSMDatabase db) {
        rec = new InsertRecordGroup(db.getDatabaseId());
        freed = false;
    }
    
    InsertRecordGroup getRecord() {
        return rec;
    }
    
    /**
     * Add a new insert operation to this group
     * @param indexId the index in which the key-value pair is inserted
     * @param key the key
     * @param value the value data
     */
    public void addInsert(int indexId, byte[] key, byte[] value) {
        rec.addInsert(indexId, key, value);
    }
    
    public void addDelete(int indexId, byte[] key) {
        rec.addInsert(indexId, key, null);
    }
    
    public String toString() {
        return rec.toString();
    }
}
