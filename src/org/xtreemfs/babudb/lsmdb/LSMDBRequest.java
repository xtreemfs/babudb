/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.lsmdb;

import org.xtreemfs.babudb.BabuDBRequestListener;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker.RequestOperation;

/**
 *
 * @author bjko
 */
public class LSMDBRequest {
    
    private final BabuDBRequestListener listener;
    private final LSMDatabase       database;
    private final int               indexId;
    private final RequestOperation  operation;
    private final InsertRecordGroup insertData;
    private final byte[]            lookupKey;
    private final Object            context;

    public LSMDBRequest(LSMDatabase database, BabuDBRequestListener listener,
            InsertRecordGroup insert, Object context) {
        this.operation = RequestOperation.INSERT;
        this.database = database;
        this.indexId = 0;
        this.insertData = insert;
        this.lookupKey = null;
        this.listener = listener;
        this.context = context;
    }

    public LSMDBRequest(LSMDatabase database, int indexId, BabuDBRequestListener listener,
            byte[] key, boolean prefix, Object context) {
        this.operation = prefix ? RequestOperation.PREFIX_LOOKUP : RequestOperation.LOOKUP;
        this.database = database;
        this.indexId = indexId;
        this.lookupKey = key;
        this.insertData = null;
        this.listener = listener;
        this.context = context;
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

    public BabuDBRequestListener getListener() {
        return listener;
    }

    public Object getContext() {
        return context;
    }
}
