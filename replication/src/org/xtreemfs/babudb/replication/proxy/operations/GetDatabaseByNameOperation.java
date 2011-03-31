/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy.operations;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Database;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.DatabaseName;
import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.transmission.ErrorCode;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;

import com.google.protobuf.Message;

/**
 * Operation to retrieve the id of an available DB remotely at the server with
 * master privilege. 
 *
 * @author flangner
 * @since 01/19/2011
 */
public class GetDatabaseByNameOperation extends Operation {

    private final BabuDBInterface dbs;
    
    public GetDatabaseByNameOperation(BabuDBInterface dbs) {
        this.dbs = dbs;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return RemoteAccessServiceConstants.PROC_ID_GETDATABASEBYNAME;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          startRequest(
     *          org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public void startRequest(final Request rq) {
        String dbName = ((DatabaseName) rq.getRequestMessage()).getDatabaseName();
        try {
            
            rq.sendSuccess(Database.newBuilder()
                    .setDatabaseName(dbName)
                    .setDatabaseId(dbs.getDatabase(dbName).getLSMDB().getDatabaseId())
                    .build());
        } catch (BabuDBException e) {
            
            rq.sendSuccess(Database.newBuilder().setErrorCode(ErrorCode.DB_UNAVAILABLE)
                    .setDatabaseId(-1).setDatabaseName(dbName).build());
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return DatabaseName.getDefaultInstance();
    }
}
