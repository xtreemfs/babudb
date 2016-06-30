/*
 * Copyright (c) 2008 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.lsmdb;

import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;


/**
 *
 * @author bjko
 */
public class BabuDBInsertGroup implements DatabaseInsertGroup {

    private final InsertRecordGroup rec;
        
    BabuDBInsertGroup(LSMDatabase db) {
        this(db.getDatabaseId());
    }
    
    BabuDBInsertGroup(int databaseId) {
        rec = new InsertRecordGroup(databaseId);
    }
    
    public InsertRecordGroup getRecord() {
        return rec;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.InsertGroup#addInsert(int, byte[], byte[])
     */
    @Override
    public void addInsert(int indexId, byte[] key, byte[] value) {
        rec.addInsert(indexId, key, value);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.InsertGroup#addDelete(int, byte[])
     */
    @Override
    public void addDelete(int indexId, byte[] key) {
        rec.addInsert(indexId, key, null);
    }
    
    public String toString() {
        return rec.toString();
    }
    
    public static BabuDBInsertGroup createInsertGroup(int dbId) {
        return new BabuDBInsertGroup(dbId);
    }
}
