/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.lsmdb;

import org.xtreemfs.babudb.BabuDBRequest;
import org.xtreemfs.babudb.UserDefinedLookup;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker.RequestOperation;

/**
 *
 * @author bjko
 * @param <T>
 */
public class LSMDBRequest<T> {
    
    private final BabuDBRequest<T>     listener;
    private final LSMDatabase       database;
    private final int               indexId;
    private final RequestOperation  operation;
    private final InsertRecordGroup insertData;
    private final byte[]            lookupKey;
    private final UserDefinedLookup udLookup;

    public LSMDBRequest(BabuDBRequest<T> listener) {
        this.operation = RequestOperation.INSERT;
        this.listener = listener;
        this.udLookup = null;
        this.lookupKey = null;
        this.insertData = null;
        this.indexId = -1;
        this.database = null;
    }
    
    public LSMDBRequest(LSMDatabase database, BabuDBRequest<T> listener,
            InsertRecordGroup insert) {
        this.operation = RequestOperation.INSERT;
        this.database = database;
        this.indexId = 0;
        this.insertData = insert;
        this.lookupKey = null;
        this.listener = listener;
        this.udLookup = null;
    }

    public LSMDBRequest(LSMDatabase database, int indexId, BabuDBRequest<T> listener,
            byte[] key, boolean prefix) {
        this.operation = prefix ? RequestOperation.PREFIX_LOOKUP : RequestOperation.LOOKUP;
        this.database = database;
        this.indexId = indexId;
        this.lookupKey = key;
        this.insertData = null;
        this.listener = listener;
        this.udLookup = null;
    }
    
    public LSMDBRequest(LSMDatabase database, BabuDBRequest<T> listener, UserDefinedLookup udLookup) {
        this.operation = RequestOperation.USER_DEFINED_LOOKUP;
        this.database = database;
        this.indexId = 0;
        this.lookupKey = null;
        this.insertData = null;
        this.listener = listener;
        this.udLookup = udLookup;
    }

    public LSMDatabase getDatabase() {
        return database;
    }

    public int getIndexId() {
        return indexId;
    }

    public RequestOperation getOperation() {
        return operation;
    }

    public InsertRecordGroup getInsertData() {
        return insertData;
    }

    public byte[] getLookupKey() {
        return lookupKey;
    }

    public BabuDBRequest<T> getListener() {
        return listener;
    }
   
    public UserDefinedLookup getUserDefinedLookup() {
        return this.udLookup;
    }
}
