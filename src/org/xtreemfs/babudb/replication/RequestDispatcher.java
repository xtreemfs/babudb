/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ProtocolException;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.errnoException;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.stateRequest;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCRequestHeader;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCResponseHeader;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.operations.ChunkOperation;
import org.xtreemfs.babudb.replication.operations.FleaseOperation;
import org.xtreemfs.babudb.replication.operations.LoadOperation;
import org.xtreemfs.babudb.replication.operations.LocalTimeOperation;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.operations.ReplicaOperation;
import org.xtreemfs.babudb.replication.operations.StateOperation;
import org.xtreemfs.babudb.replication.stages.StageRequest;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.ErrNo;
import org.xtreemfs.include.foundation.oncrpc.server.ONCRPCRequest;
import org.xtreemfs.include.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.include.foundation.oncrpc.server.RPCNIOSocketServer;
import org.xtreemfs.include.foundation.oncrpc.server.RPCServerRequestListener;

import yidl.runtime.Object;

/**
 * <p>
 * Starts necessary replication services and dispatches incoming requests.
 * </p>
 * 
 * @since 05/02/2009
 * @author flangner
 */

public abstract class RequestDispatcher implements RPCServerRequestListener {
    
    public final static String              VERSION = "1.0.0 (v1.0 RC1)";
    public final String                     name;
    
    /** table of available Operations */
    private final Map<Integer, Operation>   operations;
        
    /** outgoing requests */
    protected final RPCNIOSocketClient      rpcClient;

    /** incoming operations */
    private final RPCNIOSocketServer        rpcServer;
    
    /** a list of permitted clients */
    protected final Set<InetAddress>        permittedClients; 

    /** interface for babuDB core-components */
    public final BabuDB                     dbs;
    
    /** the last acknowledged LSN of the last view */
    public final AtomicReference<LSN>       lastOnView;
    
    /** initialized with false, a suspended dispatcher could not be suspended 
     *  again 
     */
    protected final AtomicBoolean           suspended = new AtomicBoolean();

    protected final TimeDriftDetector       timeDriftDetector;
    
    /** the replication control layer */
    private final ReplicationControlLayer   replCtl;
    
/*
 * constructors
 */
    
    /**
     * Initializing of the RequestDispatcher.
     * 
     * @param name
     * @param dbs
     * @param initial
     * @param listener 
     * @throws IOException
     */
    protected RequestDispatcher(String name, BabuDB dbs, LSN initial, 
            ReplicationControlLayer replCtl) throws IOException {
        assert(initial != null);
        
        this.replCtl = replCtl;
        this.name = name;
        this.dbs = dbs;
        this.permittedClients = new HashSet<InetAddress>();
        
        // the sequence number of the initial LSN before incrementing the 
        // viewID must not be 0 
        Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                "Setting last on view LSN to '%s', initial was '%s'.", 
        	(initial.getSequenceNo() == 0L) ? 
        	        new LSN(0,0L) : initial,initial);
        this.lastOnView = new AtomicReference<LSN>(
                (initial.getSequenceNo() == 0L) ? new LSN(0,0L) : initial);
        
        // ---------------------
        // initialize operations
        // ---------------------
        operations = initializeOperations();
        
        // -------------------------------
        // initialize communication stages
        // -------------------------------
        rpcServer = new RPCNIOSocketServer(replCtl.configuration.getPort(), 
                replCtl.configuration.getInetSocketAddress().getAddress(), this, 
                replCtl.configuration.getSSLOptions());
        rpcServer.setLifeCycleListener(replCtl);
        
        rpcClient = new RPCNIOSocketClient(replCtl.configuration.getSSLOptions(), 
                ReplicationConfig.REQUEST_TIMEOUT, 
                ReplicationConfig.CONNECTION_TIMEOUT);
        rpcClient.setLifeCycleListener(replCtl);
        
        // -------------------------------
        // fill the permitted clients list
        // -------------------------------
        for (InetSocketAddress participant : replCtl.configuration.getParticipants()) {
            assert (!participant.equals(replCtl.configuration.getInetSocketAddress()));
            permittedClients.add(participant.getAddress());
        }
        
        this.timeDriftDetector = new TimeDriftDetector(replCtl, 
                replCtl.configuration.getParticipants(), rpcClient, 
                replCtl.configuration.getLocalTimeRenew());
    }
    
    /**
     * Uses the given old dispatcher to copy its fields for the new one.
     * 
     * @param name
     * @param oldDispatcher
     */
    protected RequestDispatcher(String name, RequestDispatcher oldDispatcher) {
        this.replCtl = oldDispatcher.replCtl;
        this.timeDriftDetector = oldDispatcher.timeDriftDetector;
        
        // ---------------------
        // initialize operations
        // ---------------------
        this.operations = initializeOperations();
        
        this.rpcServer = oldDispatcher.rpcServer;
        this.rpcServer.updateRequestDispatcher(this);
        this.rpcClient = oldDispatcher.rpcClient;
        this.permittedClients = oldDispatcher.permittedClients;
        this.name = name;
        this.dbs = oldDispatcher.dbs;
        
        // the sequence number of the initial LSN before incrementing the 
        // viewID must not be 0 
        Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                "Setting last on view LSN to '%s', parameterized was.", 
                oldDispatcher.lastOnView.get());
        this.lastOnView = oldDispatcher.lastOnView;
    }

    /**
     * Registers a listener to notify, if the latest common {@link LSN} has 
     * changed. Listeners will be registered in natural order of their LSNs.
     * 
     * @param listener
     */
    abstract void subscribeListener(LatestLSNUpdateListener listener);

    /**
     * Replicate the given LogEntry. Checks for suspension.
     * 
     * @param buffer - the serialized {@link LogEntry} to replicate.
     * @param le - the original {@link LogEntry}.
     * 
     * @return the {@link ReplicateResponse}.
     */
    ReplicateResponse replicate(LogEntry le, ReusableBuffer buffer) {
        synchronized (suspended) {
            if (!suspended.get())
                return _replicate(le, buffer);
            else
                return new ReplicateResponse(le, new IOException(
                        "Replication is suspended at the moment. " +
                        "Try again later."));   
        }
    }
    
    /**
     * Replicate the given LogEntry.
     * 
     * @param buffer - the serialized {@link LogEntry} to replicate.
     * @param le - the original {@link LogEntry}.
     * 
     * @return the {@link ReplicateResponse}.
     */
    abstract ReplicateResponse _replicate(LogEntry le, ReusableBuffer buffer);
    
    /**
     * Register the events at the operation's table. 
     */
    private Map<Integer,Operation> initializeOperations() {
        Map<Integer, Operation> result = new HashMap<Integer, Operation>();
        Operation op = new LocalTimeOperation();
        result.put(op.getProcedureId(), op);
        
        op = new StateOperation(this);
        result.put(op.getProcedureId(), op);
        
        if (!replCtl.quit) {
            op = new FleaseOperation(replCtl.fleaseStage);
            result.put(op.getProcedureId(), op);
        }
        
        op = new ReplicaOperation(this);
        result.put(op.getProcedureId(),op);
        
        op = new LoadOperation(this);
        result.put(op.getProcedureId(),op);
        
        op = new ChunkOperation();
        result.put(op.getProcedureId(),op);
        
        additionalOperations(result);
        
        return result;
    }
    
    /**
     * Register additional events at the operation's table. 
     * 
     *  @param operations
     */
    protected abstract void additionalOperations(
            Map<Integer, Operation> operations);
        
    /**
     * Initializes the dispatcher services.
     */
    void start() {
        // operations must not be changed while the rpcServer is running!
        Operation op = new FleaseOperation(replCtl.fleaseStage);
        operations.put(op.getProcedureId(), op);
        
        try {  
            rpcServer.start();
            rpcServer.waitForStartup();
            
            rpcClient.start();
            rpcClient.waitForStartup();           
        } catch (Exception ex) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "startup failed");
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
            System.exit(1);
        }
    }
    
    void asyncShutdown() {
        try {
            timeDriftDetector.shutdown();
            rpcServer.shutdown();
            rpcClient.shutdown();
        } catch (Exception ex) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "shutdown failed");
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
        }
    }
    
    void shutdown() {
        try {    
            timeDriftDetector.shutdown();
            
            rpcServer.shutdown();
            rpcServer.waitForShutdown();
            
            rpcClient.shutdown();
            rpcClient.waitForShutdown();  
        } catch (Exception ex) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "shutdown failed");
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.include.foundation.oncrpc.server.
     * RPCServerRequestListener#receiveRecord(org.xtreemfs.include.
     * foundation.oncrpc.server.ONCRPCRequest)
     */
    @Override
    public void receiveRecord(ONCRPCRequest rq){    
        if (!checkIdentity(rq.getClientIdentity())){
            rq.sendException(new ProtocolException(
                    ONCRPCResponseHeader.ACCEPT_STAT_PROC_UNAVAIL,
                    ErrNo.EACCES,"you "+rq.getClientIdentity().toString()+
                    " have no access rights to execute the requested operation"+
                    " on this "+name));
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
                    ") is not available on this "+name));
            return;
        } 
        
        Request rpcrq = new Request(rq);
        try {
            Object message = op.parseRPCMessage(rpcrq);
            if (message!=null) throw new Exception(message.getTypeName());
        } catch (Throwable ex) {
            rq.sendGarbageArgs(ex.toString(),new ProtocolException(
                    ONCRPCResponseHeader.ACCEPT_STAT_SYSTEM_ERR, 
                    ErrNo.EINVAL,"message could not be retrieved"));
            return;
        }
        
        try {
            op.startRequest(rpcrq);
        } catch (Throwable ex) {
            rq.sendInternalServerError(ex,new errnoException(
                    ErrNo.ENOSYS,ex.getMessage(),null));
            return;
        }
    }

    /**
     * Simple security check by identifying the client.
     * 
     * @param clientIdentity
     * @return true if the client could be identified, false otherwise.
     */
    boolean checkIdentity (SocketAddress clientIdentity) {
        if (clientIdentity instanceof InetSocketAddress) {
            return permittedClients.contains(
                    ((InetSocketAddress) clientIdentity).getAddress());
        }
        Logging.logMessage(Logging.LEVEL_ERROR, this, 
                "Access-rights for client: '" + clientIdentity + 
                "' could not be validated.");
        return false;
    }
    
    /**
     * @return the number of connected clients.
     */
    int getNumClientConnections() {
        return rpcServer.getNumConnections();
    }

    /**
     * @return the number of pending requests.
     */
    long getPendingRequests() {
        return rpcServer.getPendingRequests();
    }
    
    /**
     * @return the currently used {@link ReplicationConfig}.
     */
    public ReplicationConfig getConfig() {
        return replCtl.configuration;
    }
    
/*
 * State interface operations
 */
    /**
     * Needed to respond {@link stateRequest}s.
     * 
     * @return the {@link DispatcherState} of the {@link RequestDispatcher}, 
     *         including the {@link LSN} of the last locally written 
     *         {@link LogEntry}.
     */
    public abstract DispatcherState getState();
    
    /**
     * Stops the dispatcher, by disabling all its replication features.
     */
    public void suspend() {
        synchronized (suspended) {
            if (this.suspended.compareAndSet(false, true)) _suspend();   
        }
    }
    
    /**
     * Stops the dispatcher, by disabling all its replication features.
     */
    abstract void _suspend();
    
    /**
     * State of the dispatcher after shutting it down.
     * 
     * @author flangner
     * @since 08/07/2009
     */
    public static class DispatcherState {
        
        public LSN latest;
        public BlockingQueue<StageRequest> requestQueue;
        
        DispatcherState(LSN latest, BlockingQueue<StageRequest> backupQueue) {
            this.latest = latest;
            this.requestQueue = backupQueue;
        }
        
        DispatcherState(LSN latest) {
            this.latest = latest;
            this.requestQueue = null;
        }
        
        /*
         * (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "LSN ("+latest.toString()+")" + (
                (requestQueue!=null) ? ", Queue-length: '" + requestQueue.size()
                        + "'" : "");
        }
    }
}
