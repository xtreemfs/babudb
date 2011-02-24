/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission.dispatcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.replication.Layer;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.MessageType;
import org.xtreemfs.foundation.pbrpc.server.RPCServerRequest;
import org.xtreemfs.foundation.pbrpc.server.RPCNIOSocketServer;
import org.xtreemfs.foundation.pbrpc.server.RPCServerRequestListener;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader;

import static org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.POSIXErrno.POSIX_ERROR_NONE;

/**
 * Dispatches incoming requests.
 * 
 * @since 05/02/2009
 * @author flangner
 */

public class RequestDispatcher implements RPCServerRequestListener {

    /** incoming operations */
    private final RPCNIOSocketServer            rpcServer;
   
    /** table of available request handlers */
    private final Map<Integer, RequestHandler>  handlers = 
        new HashMap<Integer, RequestHandler>();
    
    /**
     * Initializing of the RequestDispatcher.
     * 
     * @param config
     * @throws IOException
     */
    public RequestDispatcher(ReplicationConfig config) throws IOException {
        
        rpcServer = new RPCNIOSocketServer(config.getPort(), config.getAddress(), 
                this, config.getSSLOptions());
    }
    
    /**
     * Add some logical handler for the requests.
     * 
     * @param handler - the handler to handle the requests.. for sure!
     */
    public void addHandler(RequestHandler handler) {
        handlers.put(handler.getInterfaceID(), handler);
    }
    
    /**
     * <p>
     * Sets the given {@link LifeCycleListener} for all {@link LifeCycleThread}s
     * of this {@link Layer}.
     * </p>
     * 
     * @param listener - the {@link LifeCycleListener}.
     */
    public void setLifeCycleListener(LifeCycleListener listener) {
        rpcServer.setLifeCycleListener(listener);
    }
        
    /**
     * Initializes the dispatcher services.
     */
    public void start() {       
        try {  
            if (handlers.size() == 0) {
                throw new Exception("The dispatcher cannot be started, " +
                                    "without any handler registered at!");
            }
            
            rpcServer.start();
            rpcServer.waitForStartup();
        } catch (Exception ex) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "startup failed");
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
            throw new RuntimeException("Fatal error while initializing the " +
            		"replication plugin. Check log for details.");
        }
    }
    
    public void waitForStartup() throws Exception {
        rpcServer.waitForStartup();
    }
    
    public void waitForShutdown() throws Exception {
        rpcServer.waitForShutdown();
    }
    
    public void shutdown() {
        rpcServer.shutdown();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.pbrpc.server.RPCServerRequestListener#
     * receiveRecord(org.xtreemfs.foundation.pbrpc.server.RPCServerRequest)
     */
    @Override
    public void receiveRecord(RPCServerRequest rq) {  
              
        Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                "Dispatching request %s ...", rq.toString());
        
        RPCHeader hdr = rq.getHeader();
        
        if (hdr.getMessageType() != MessageType.RPC_REQUEST) {
            rq.sendError(ErrorType.GARBAGE_ARGS, POSIX_ERROR_NONE, 
                    "expected RPC request message type but got " + 
                    hdr.getMessageType());
            return;
        }
        
        int interfaceId = hdr.getRequestHeader().getInterfaceId();
        
        RequestHandler handler = handlers.get(interfaceId);
        if (handler == null) {
            rq.sendError(ErrorType.INVALID_PROC_ID, POSIX_ERROR_NONE,
                    "requested handler (#" + interfaceId + 
                    ") is not accessible");
            return;
        } 
        
        handler.handleRequest(rq);
    }
}
