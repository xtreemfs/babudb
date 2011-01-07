/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission.dispatcher;

import java.io.IOException;
import java.util.Map;

import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.Layer;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsVerification;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.MessageType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader.RequestHeader;
import org.xtreemfs.foundation.pbrpc.server.RPCServerRequest;
import org.xtreemfs.foundation.pbrpc.server.RPCNIOSocketServer;
import org.xtreemfs.foundation.pbrpc.server.RPCServerRequestListener;
import org.xtreemfs.foundation.util.OutputUtils;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader;

import static org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.POSIXErrno.POSIX_ERROR_NONE;

/**
 * Dispatches incoming requests.
 * 
 * @since 05/02/2009
 * @author flangner
 */

public class RequestDispatcher implements RPCServerRequestListener {
    
    /** table of available Operations */
    private volatile Map<Integer, Operation>    operations = null;
    
    /** object to check if a requesting client is a replication participant */
    private volatile ParticipantsVerification   verificator = null;

    /** incoming operations */
    private final RPCNIOSocketServer            rpcServer;
   
    /**
     * Initializing of the RequestDispatcher.
     * 
     * @param config
     * @throws IOException
     */
    public RequestDispatcher(ReplicationConfig config) throws IOException {
        
        this.rpcServer = new RPCNIOSocketServer(config.getPort(), 
                config.getInetSocketAddress().getAddress(), this, 
                config.getSSLOptions());
    }
    
    /**
     * Registers the given {@link ParticipantsVerification} verificator at the
     * dispatcher.
     * 
     * @param verificator
     */
    public synchronized void registerVerificator(
            ParticipantsVerification verificator) {
        assert(verificator != null);
        
        if (this.verificator == null)
            this.verificator = verificator;
    }
    
    /**
     * Registers the set of operations at the dispatcher.
     * 
     * @param operations
     */
    public synchronized void setOperations(Map<Integer, Operation> operations) {
        assert(operations != null);
        
        if (this.verificator == null)
            this.operations = operations;
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
        this.rpcServer.setLifeCycleListener(listener);
    }
        
    /**
     * Initializes the dispatcher services.
     */
    public void start() {       
        try {  
            if (this.operations == null)
                throw new Exception("The dispatcher cannot be started, " +
                		"without any operations registered at!");
            
            if (this.verificator == null)
                throw new Exception("The dispatcher cannot be started, " +
                		"without a verificator registered at!");
            
            this.rpcServer.start();
            this.rpcServer.waitForStartup();
        } catch (Exception ex) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "startup failed");
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
            System.exit(1);
        }
    }
    
    public void waitForStartup() throws Exception {
        this.rpcServer.waitForStartup();
    }
    
    public void waitForShutdown() throws Exception {
        this.rpcServer.waitForShutdown();
    }
    
    public void shutdown() {
        this.rpcServer.shutdown();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.pbrpc.server.RPCServerRequestListener#
     * receiveRecord(org.xtreemfs.foundation.pbrpc.server.RPCServerRequest)
     */
    @Override
    public void receiveRecord(RPCServerRequest rq) {  
        
        if (!verificator.isRegistered(rq.getSenderAddress())){
            rq.sendError(ErrorType.AUTH_FAILED, POSIX_ERROR_NONE, "you " 
                    + rq.getSenderAddress().toString() + " have no access " 
                    + "rights to execute the requested operation");
            return;
        }
              
        RPCHeader hdr = rq.getHeader();
        
        if (hdr.getMessageType() != MessageType.RPC_REQUEST) {
            rq.sendError(ErrorType.GARBAGE_ARGS, POSIX_ERROR_NONE, 
                    "expected RPC request message type but got " + 
                    hdr.getMessageType());
            return;
        }
        
        final RequestHeader rqHdr = hdr.getRequestHeader();
        
        if (rqHdr.getInterfaceId() != ReplicationServiceConstants.INTERFACE_ID){
            rq.sendError(ErrorType.INVALID_INTERFACE_ID, null,
                    "invalid interface id");
            return;
        }
        
        Operation op = operations.get(rqHdr.getProcId());
        if (op == null) {
            rq.sendError(ErrorType.INVALID_PROC_ID, POSIX_ERROR_NONE,
                    "requested operation (" + rqHdr.getProcId() + 
                    ") is not available");
            return;
        } 
        
        Request rpcrq = new Request(rq);
        try {
            Object message = op.parseRPCMessage(rpcrq);
            if (message != null) throw new Exception();
        } catch (Throwable ex) {
            rq.sendError(ErrorType.GARBAGE_ARGS, POSIX_ERROR_NONE, 
                    "message could not be parsed");
            return;
        }
        
        try {
            op.startRequest(rpcrq);
        } catch (Throwable ex) {
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
            
            rq.sendError(ErrorType.INTERNAL_SERVER_ERROR, POSIX_ERROR_NONE, 
                    "internal server error: "+ex.toString(), 
                    OutputUtils.stackTraceToString(ex));
            return;
        }
    }
}
