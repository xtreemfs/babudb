/*
 * Copyright (c) 2009-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy.operations;

import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.EntryMap;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.RangeLookup;
import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.transmission.ErrorCode;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

import com.google.protobuf.Message;

/**
 * Operation to handle a remote lookup at the server with master privilege. 
 *
 * @author flangner
 * @since 01/19/2011
 */
public class RangeLookupOperation extends Operation {

    private final BabuDBInterface dbs;
    
    public RangeLookupOperation(BabuDBInterface dbs) {
        this.dbs = dbs;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return RemoteAccessServiceConstants.PROC_ID_RLOOKUP;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          startRequest(
     *          org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public void startRequest(final Request rq) {
        RangeLookup req = (RangeLookup) rq.getRequestMessage();
        
        int limit = req.getFromLength();       
        ReusableBuffer f = rq.getRpcRequest().getData().createViewBuffer();
        f.limit(limit);
        byte[] from = f.getData();
        ReusableBuffer t = rq.getRpcRequest().getData().createViewBuffer();
        t.position(limit);
        byte[] to = t.getData();
       
        try {
            dbs.getDatabase(req.getDatabaseName()).rangeLookup(req.getIndexId(), 
                            from, to, null).registerListener(
                                        new DatabaseRequestListener<Iterator<Entry<byte[], byte[]>>>() {
                
                @Override
                public void finished(Iterator<Entry<byte[], byte[]>> result, Object context) {
                    EntryMap.Builder r = EntryMap.newBuilder();
                    ReusableBuffer data = BufferPool.allocate(0);
                    
                    while (result.hasNext()) {
                        Entry<byte[], byte[]> entry = result.next();
                        ReusableBuffer key = ReusableBuffer.wrap(entry.getKey());
                        ReusableBuffer value = ReusableBuffer.wrap(entry.getValue());
                        
                        r.addLength(key.remaining());
                        r.addLength(value.remaining());
                        
                        int newSize = data.remaining() + key.remaining() + 
                                      value.remaining();
                        if (!data.enlarge(newSize)) {
                            ReusableBuffer tmp = BufferPool.allocate(newSize);
                            
                            tmp.put(data);
                            BufferPool.free(data);
                            data = tmp;
                        }
                        data.put(key);
                        data.put(value);
                    }
                    
                    rq.sendSuccess(r.build(), data);
                }
                
                @Override
                public void failed(BabuDBException error, Object context) {
                    rq.sendSuccess(ErrorCodeResponse.newBuilder().setErrorCode(
                            ErrorCode.ENTRY_UNAVAILABLE).build());
                }
            });
        } catch (BabuDBException e) {
            rq.sendSuccess(ErrorCodeResponse.newBuilder().setErrorCode(
                    ErrorCode.DB_UNAVAILABLE).build());
        } finally {
            BufferPool.free(f);
            BufferPool.free(t);
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return RangeLookup.getDefaultInstance();
    }
}
