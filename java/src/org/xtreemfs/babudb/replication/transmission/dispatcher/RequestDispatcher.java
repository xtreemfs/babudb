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
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ProtocolException;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.errnoException;
import org.xtreemfs.babudb.replication.Layer;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsVerification;
import org.xtreemfs.foundation.ErrNo;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.oncrpc.server.NullAuthFlavorProvider;
import org.xtreemfs.foundation.oncrpc.server.ONCRPCRequest;
import org.xtreemfs.foundation.oncrpc.server.RPCNIOSocketServer;
import org.xtreemfs.foundation.oncrpc.server.RPCServerRequestListener;
import org.xtreemfs.foundation.util.OutputUtils;
import org.xtreemfs.foundation.oncrpc.utils.ONCRPCRequestHeader;
import org.xtreemfs.foundation.oncrpc.utils.ONCRPCResponseHeader;

import yidl.runtime.Object;

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
                config.getSSLOptions(), 
                new NullAuthFlavorProvider());
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

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.oncrpc.server.
     * RPCServerRequestListener#receiveRecord(org.xtreemfs.include.
     * foundation.oncrpc.server.ONCRPCRequest)
     */
    @Override
    public void receiveRecord(ONCRPCRequest rq){    
        if (!verificator.isRegistered(rq.getClientIdentity())){
            rq.sendException(new ProtocolException(
                    ONCRPCResponseHeader.ACCEPT_STAT_PROC_UNAVAIL,
                    ErrNo.EACCES,"you "+rq.getClientIdentity().toString()+
                    " have no access rights to execute the requested " +
                    "operation"));
            return;
        }
                
        final ONCRPCRequestHeader hdr = rq.getRequestHeader();
        
        if (hdr.getInterfaceVersion() != ReplicationInterface.getVersion()) {
            rq.sendException(new ProtocolException(
                    ONCRPCResponseHeader.ACCEPT_STAT_PROG_MISMATCH,
                    ErrNo.EINVAL,"invalid version requested"));
            return;
        }
        
        Operation op = operations.get(hdr.getTag());
        if (op == null) {
            rq.sendException(new ProtocolException(
                    ONCRPCResponseHeader.ACCEPT_STAT_PROC_UNAVAIL,
                    ErrNo.EINVAL,"requested operation ("+hdr.getTag()+
                    ") is not available"));
            return;
        } 
        
        Request rpcrq = new Request(rq);
        try {
            Object message = op.parseRPCMessage(rpcrq);
            if (message!=null) throw new Exception(message.getTypeName());
        } catch (Throwable ex) {
            rq.sendGarbageArgs();
            return;
        }
        
        try {
            op.startRequest(rpcrq);
        } catch (Throwable ex) {
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
            
            rq.sendException(new errnoException(ErrNo.EIO, 
                    "internal server error: "+ex.toString(), 
                    OutputUtils.stackTraceToString(ex)));
            return;
        }
    }
}
