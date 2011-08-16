/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy.operations;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.pbrpc.Common.emptyRequest;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Database;
import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.transmission.ErrorCode;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.logging.Logging;

import com.google.protobuf.Message;

/**
 * Operation to execute database modifying methods remotely at the server with
 * master privilege. 
 *
 * @author flangner
 * @since 01/19/2011
 */
public class MakePersistentOperation extends Operation {

    private final BabuDBInterface dbs;
    
    public MakePersistentOperation(BabuDBInterface dbs) {
        this.dbs = dbs;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return RemoteAccessServiceConstants.PROC_ID_MAKEPERSISTENT;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return emptyRequest.getDefaultInstance();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          processRequest(org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public void processRequest(final Request rq) {
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "MakePersistentOperation");
        
        try {
            BabuDBRequestResultImpl<Object> result = dbs.createRequestFuture();
            dbs.getTransactionManager().makePersistent(rq.getData().createViewBuffer(), result);
            result.registerListener(new DatabaseRequestListener<Object>() {
                
                @Override
                public void finished(Object result, Object context) {
                    
                    // TODO generalize logic to process transaction results
                    int dbId = -1;
                    String dbName = "\0";
                    if (result instanceof Object[] && 
                      ((Object[]) result).length > 0 && 
                      ((Object[]) result)[0] != null && 
                      ((Object[]) result)[0] instanceof DatabaseInternal) {
                        
                        DatabaseInternal dbInternal = (DatabaseInternal) ((Object[]) result)[0];
                        dbId = dbInternal.getLSMDB().getDatabaseId();
                        dbName = dbInternal.getName();
                    }
                    
                    rq.sendSuccess(Database.newBuilder()
                            .setDatabaseId(dbId).setDatabaseName(dbName).build());
                }
                
                @Override
                public void failed(BabuDBException error, Object context) {
                    rq.sendSuccess(Database.newBuilder()
                            .setErrorCode(ErrorCode.mapUserError(error))
                            .setDatabaseId(-1)
                            .setDatabaseName("\0").build());
                }
            });
        } catch (BabuDBException error) {
            rq.sendSuccess(Database.newBuilder()
                    .setErrorCode(ErrorCode.mapUserError(error))
                    .setDatabaseId(-1)
                    .setDatabaseName("\0").build());
        } 
    }
}
