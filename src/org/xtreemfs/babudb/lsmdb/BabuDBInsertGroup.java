/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
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
    
    private boolean freed;
    
    BabuDBInsertGroup(LSMDatabase db) {
        rec = new InsertRecordGroup(db.getDatabaseId());
        freed = false;
    }
    
    InsertRecordGroup getRecord() {
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
}
