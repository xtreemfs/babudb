/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy.operations;

import java.util.Map;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.pbrpc.Common.emptyRequest;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Databases;
import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.logging.Logging;

import com.google.protobuf.Message;

/**
 * Operation to retrieve a list of available DBs remotely at the server with
 * master privilege. 
 *
 * @author flangner
 * @since 01/19/2011
 */
public class GetDatabasesOperation extends Operation {

    private final BabuDBInterface dbs;
    
    public GetDatabasesOperation(BabuDBInterface dbs) {
        this.dbs = dbs;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return RemoteAccessServiceConstants.PROC_ID_GETDATABASES;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return emptyRequest.getDefaultInstance();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#processRequest(org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public void processRequest(Request rq) {
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "GetDatabasesOperation.");
        
        Databases.Builder rBuilder = Databases.newBuilder();
        Map<String, DatabaseInternal> databases = dbs.getDatabases();
        for (Entry<String, DatabaseInternal> e : databases.entrySet()) {
            rBuilder.addDatabase(org.xtreemfs.babudb.pbrpc.GlobalTypes.Database.newBuilder()
                    .setDatabaseName(e.getKey())
                    .setDatabaseId(e.getValue().getLSMDB().getDatabaseId())
                    .build());
        }
        rq.sendSuccess(rBuilder.build());
    }
}
