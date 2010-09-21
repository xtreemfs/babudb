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
    
    private final BabuDBRequest<T>  listener;
    
    private final LSMDatabase       database;
    
    private final int               indexId;
    
    private final RequestOperation  operation;
    
    private final InsertRecordGroup insertData;
    
    private final byte[]            lookupKey;
    
    private byte[]                  from;
    
    private byte[]                  to;
    
    private boolean                 ascending;
    
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
    
    public LSMDBRequest(LSMDatabase database, BabuDBRequest<T> listener, InsertRecordGroup insert) {
        this.operation = RequestOperation.INSERT;
        this.database = database;
        this.indexId = 0;
        this.insertData = insert;
        this.lookupKey = null;
        this.listener = listener;
        this.udLookup = null;
    }
    
    public LSMDBRequest(LSMDatabase database, int indexId, BabuDBRequest<T> listener, byte[] key) {
        this.operation = RequestOperation.LOOKUP;
        this.database = database;
        this.indexId = indexId;
        this.lookupKey = key;
        this.insertData = null;
        this.listener = listener;
        this.udLookup = null;
    }
    
    public LSMDBRequest(LSMDatabase database, int indexId, BabuDBRequest<T> listener, byte[] prefix,
        boolean ascending) {
        this.operation = RequestOperation.PREFIX_LOOKUP;
        this.database = database;
        this.indexId = indexId;
        this.lookupKey = prefix;
        this.insertData = null;
        this.listener = listener;
        this.udLookup = null;
        this.ascending = ascending;
    }
    
    public LSMDBRequest(LSMDatabase database, int indexId, BabuDBRequest<T> listener, byte[] from, byte[] to, boolean ascending) {
        this.operation = RequestOperation.RANGE_LOOKUP;
        this.database = database;
        this.indexId = indexId;
        this.from = from;
        this.to = to;
        this.lookupKey = null;
        this.insertData = null;
        this.listener = listener;
        this.udLookup = null;
        this.ascending = ascending;
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
    
    public byte[] getFrom() {
        return from;
    }
    
    public byte[] getTo() {
        return to;
    }
    
    public boolean isAscending() {
        return ascending;
    }
    
    public BabuDBRequest<T> getListener() {
        return listener;
    }
    
    public UserDefinedLookup getUserDefinedLookup() {
        return this.udLookup;
    }
}
