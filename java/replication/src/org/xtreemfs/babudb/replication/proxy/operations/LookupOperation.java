/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy.operations;

import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Lookup;
import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.transmission.ErrorCode;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

import com.google.protobuf.Message;

/**
 * Operation to handle a remote lookup at the server with master privilege. 
 *
 * @author flangner
 * @since 01/19/2011
 */
public class LookupOperation extends Operation {

    private final BabuDBInterface dbs;
    
    public LookupOperation(BabuDBInterface dbs) {
        this.dbs = dbs;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return RemoteAccessServiceConstants.PROC_ID_LOOKUP;
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
        byte[] key = rq.getData().array();
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "LookupOperation:" +
                "db %s, index %d, key %s.", req.getDatabaseName(), req.getIndexId(), 
                new String(key));
        
        try {
            dbs.getDatabase(req.getDatabaseName()).lookup(req.getIndexId(), key, 
                    null).registerListener(new DatabaseRequestListener<byte[]>() {
                
                @Override
                public void finished(byte[] result, Object context) {
                    rq.sendSuccess(ErrorCodeResponse.getDefaultInstance(), (result == null) ? 
                            null : ReusableBuffer.wrap(result));
                }
                
                @Override
                public void failed(BabuDBException error, Object context) {
                    rq.sendSuccess(ErrorCodeResponse.newBuilder().setErrorCode(
                            ErrorCode.mapUserError(error.getErrorCode())).build());
                }
            });
        } catch (BabuDBException e) {
            rq.sendSuccess(ErrorCodeResponse.newBuilder().setErrorCode(
                    ErrorCode.mapUserError(e.getErrorCode())).build());
        }
    }
}
