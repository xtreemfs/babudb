/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.EntryMap;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Lookup;
import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.transmission.ErrorCode;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

import com.google.protobuf.Message;

/**
 * Operation to handle a remote lookup at the server with master privilege. 
 *
 * @author flangner
 * @since 01/19/2011
 */
public class PrefixLookupOperation extends Operation {

    private final BabuDBInterface dbs;
    
    public PrefixLookupOperation(BabuDBInterface dbs) {
        this.dbs = dbs;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return RemoteAccessServiceConstants.PROC_ID_PLOOKUP;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return Lookup.getDefaultInstance();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          processRequest(org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public void processRequest(final Request rq) {
        
        Lookup req = (Lookup) rq.getRequestMessage();
        ReusableBuffer data = rq.getData();
        byte[] key = (data != null) ? data.array() : null;
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "PrefixLookupOperation:" +
                "db %s, index %d, key %s.", req.getDatabaseName(), req.getIndexId(), 
                (key == null) ? "null" : new String(key));
        
        try {            
            dbs.getDatabase(req.getDatabaseName()).prefixLookupNonblocking(req.getIndexId(), key, 
                    null).registerListener(new DatabaseRequestListener<ResultSet<byte[], byte[]>>() {
                
                @Override
                public void finished(ResultSet<byte[], byte[]> result, Object context) {
                    EntryMap.Builder r = EntryMap.newBuilder();
                        
                    // estimate the result size
                    int size = 0;
                    List<Entry<byte[], byte[]>> tmp = new ArrayList<Entry<byte[], byte[]>>();
                    while (result.hasNext()) {
                        Entry<byte[], byte[]> entry = result.next();
                        size += entry.getKey().length + entry.getValue().length;
                        tmp.add(entry);
                    }
                    result.free();
                    
                    // prepare the response
                    ReusableBuffer data = BufferPool.allocate(size);
                    for (Entry<byte[], byte[]> entry : tmp) {
                        r.addLength(entry.getKey().length);
                        r.addLength(entry.getValue().length);
                        
                        data.put(entry.getKey());
                        data.put(entry.getValue());
                    }
                    data.flip();
                    
                    rq.sendSuccess(r.build(), data);
                }
                
                @Override
                public void failed(BabuDBException error, Object context) {
                    rq.sendSuccess(ErrorCodeResponse.newBuilder().setErrorCode(
                            ErrorCode.mapUserError(error)).build());
                }
            });
        } catch (BabuDBException e) {
            rq.sendSuccess(ErrorCodeResponse.newBuilder().setErrorCode(
                    ErrorCode.mapUserError(e)).build());
        }
    }
}
