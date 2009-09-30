/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.SimplifiedBabuDBRequestListener;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ProtocolException;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.errnoException;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.stateRequest;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCRequestHeader;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCResponseHeader;
import org.xtreemfs.babudb.interfaces.utils.Serializable;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.operations.RemoteStopOperation;
import org.xtreemfs.babudb.replication.operations.StateOperation;
import org.xtreemfs.babudb.replication.operations.ToMasterOperation;
import org.xtreemfs.babudb.replication.operations.ToSlaveOperation;
import org.xtreemfs.babudb.replication.stages.StageRequest;
import org.xtreemfs.include.common.config.ReplicationConfig;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.ErrNo;
import org.xtreemfs.include.foundation.LifeCycleListener;
import org.xtreemfs.include.foundation.oncrpc.server.ONCRPCRequest;
import org.xtreemfs.include.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.include.foundation.oncrpc.server.RPCNIOSocketServer;
import org.xtreemfs.include.foundation.oncrpc.server.RPCServerRequestListener;

/**
 * <p>Starts necessary replication services and dispatches incoming requests.</p>
 * 
 * @since 05/02/2009
 * @author flangner
 */

public abstract class RequestDispatcher implements RPCServerRequestListener, LifeCycleListener {

    public final static String              VERSION = "1.0.0 (v1.0 RC1)";
    public final String                     name;
    
    /** table of available Operations */
    protected final Map<Integer, Operation> operations;
        
    /** outgoing requests */
    public final RPCNIOSocketClient         rpcClient;

    /** incoming operations */
    private final RPCNIOSocketServer        rpcServer;
    
    /** a list of permitted clients */
    protected final List<InetAddress>       permittedClients; 

    /** interface for babuDB core-components */
    public final BabuDB                     dbs;
    
    /** determines if the replication is running in master mode */
    protected boolean                       isMaster;
    
    /** counter for eventually running requests */
    private final AtomicInteger             activeRequests = new AtomicInteger(0);
    /** flag that determines if the replication is out of service at the moment */
    public volatile boolean                 stopped = true;
    
    private SimplifiedBabuDBRequestListener listener = null;
    
    public final ReplicationConfig          configuration;
/*
 * stages
 */
    
    /**
     * Initializing of the RequestDispatcher.
     * 
     * @param name
     * @param dbs
     * @throws IOException
     */
    public RequestDispatcher(String name, BabuDB dbs) throws IOException {
        this.name = name;
        this.dbs = dbs;
        this.permittedClients = new LinkedList<InetAddress>();
        this.configuration = (ReplicationConfig) dbs.getConfig();
        
        // ---------------------
        // initialize operations
        // ---------------------
        
        operations = new HashMap<Integer, Operation>();
        
        Operation op = new StateOperation(this);
        operations.put(op.getProcedureId(), op);
        
        op = new RemoteStopOperation(this);
        operations.put(op.getProcedureId(), op);
        
        op = new ToSlaveOperation(this);
        operations.put(op.getProcedureId(), op);
        
        op = new ToMasterOperation(this);
        operations.put(op.getProcedureId(), op);
        
        initializeOperations();
        
        // -------------------------------
        // initialize communication stages
        // -------------------------------

        rpcServer = new RPCNIOSocketServer(configuration.getPort(), 
                configuration.getInetSocketAddress().getAddress(), this, 
                configuration.getSSLOptions());
        rpcServer.setLifeCycleListener(this);
        
        rpcClient = new RPCNIOSocketClient(configuration.getSSLOptions(), 
                5000, 5*60*1000);
        rpcClient.setLifeCycleListener(this);
        
        // -------------------------------
        // fill the permitted clients list
        // -------------------------------
           
        for (InetSocketAddress slave : configuration.getParticipants())
            if (!slave.equals(configuration.getInetSocketAddress()))
                permittedClients.add(slave.getAddress());
    }
    
    /**
     * Uses the given old dispatcher to copy its fields for the new one.
     * 
     * @param name
     * @param oldDispatcher
     */
    public RequestDispatcher(String name, RequestDispatcher oldDispatcher) {
        this.rpcServer = oldDispatcher.rpcServer;
        this.rpcServer.updateRequestDispatcher(this);
        this.rpcServer.setLifeCycleListener(this);
        this.rpcClient = oldDispatcher.rpcClient;
        this.rpcClient.setLifeCycleListener(this);
        this.permittedClients = oldDispatcher.permittedClients;
        this.name = name;
        this.dbs = oldDispatcher.dbs;
        this.configuration = oldDispatcher.configuration;
        
        // ---------------------
        // initialize operations
        // ---------------------
        
        this.operations = new HashMap<Integer, Operation>();
        
        Operation op = new StateOperation(this);
        this.operations.put(op.getProcedureId(), op);
        
        op = new RemoteStopOperation(this);
        this.operations.put(op.getProcedureId(), op);
        
        op = new ToSlaveOperation(this);
        this.operations.put(op.getProcedureId(), op);
        
        op = new ToMasterOperation(this);
        this.operations.put(op.getProcedureId(), op);
        
        initializeOperations();
    }

    /**
     * Replicate the given LogEntry.
     * 
     * @param le - {@link LogEntry} to replicate.
     * 
     * @throws IOException 
     * @throws NotEnoughAvailableSlavesException
     * @throws InterruptedException
     */
    abstract void _replicate(LogEntry le) throws NotEnoughAvailableSlavesException, 
        InterruptedException, IOException;

    /**
     * Replicate the given LogEntry.
     * 
     * @param le - {@link LogEntry} to replicate.
     * 
     * @throws IOException 
     * @throws NotEnoughAvailableSlavesException
     * @throws InterruptedException
     * @throws BabuDBException if request was not allowed to proceed.
     */
    public void replicate(LogEntry le) throws NotEnoughAvailableSlavesException, InterruptedException, 
                                                IOException, BabuDBException {
        if (!isMaster) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
            "This BabuDB is not running in master-mode! The operation is not available.");
        else if (!startRequest()) 
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, "Replication is disabled at the moment!");
        else {
            try {
                _replicate(le);
            } finally {
                finishRequest(true);
            }
        }
    }
    
    /**
     * Register the events at the operation's table. 
     */
    protected abstract void initializeOperations();
        
    public void start() {   
        this.stopped = false;
        try {  
            rpcServer.start();
            rpcClient.start();
            
            rpcServer.waitForStartup();
            rpcClient.waitForStartup();           
        } catch (Exception ex) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "startup failed");
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
            System.exit(1);
        }
    }
    
    public void asyncShutdown() {
        this.stopped = true;
        try {
            rpcServer.shutdown();
            rpcClient.shutdown();
        } catch (Exception ex) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "shutdown failed");
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
        }
    }
    
    public void shutdown() {
        try {    
            rpcServer.shutdown();
            rpcClient.shutdown();
            
            rpcServer.waitForShutdown();
            rpcClient.waitForShutdown();                        
        } catch (Exception ex) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "shutdown failed");
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
        }
        this.stopped = true;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.include.foundation.oncrpc.server.RPCServerRequestListener#receiveRecord(org.xtreemfs.include.foundation.oncrpc.server.ONCRPCRequest)
     */
    @Override
    public void receiveRecord(ONCRPCRequest rq){       
        if (!checkIdentity(rq.getClientIdentity())){
            rq.sendException(new ProtocolException(ONCRPCResponseHeader.ACCEPT_STAT_PROC_UNAVAIL,
                    ErrNo.EACCES,"you "+rq.getClientIdentity().toString()+" have no access rights to execute the requested operation on this "+name));
            return;
        }
                
        final ONCRPCRequestHeader hdr = rq.getRequestHeader();
        
        if (hdr.getInterfaceVersion() != ReplicationInterface.getVersion()) {
            rq.sendException(new ProtocolException(ONCRPCResponseHeader.ACCEPT_STAT_PROG_MISMATCH,
                    ErrNo.EINVAL,"invalid version requested"));
            return;
        }
        
        Operation op = operations.get(hdr.getTag());
        if (op == null) {
            rq.sendException(new ProtocolException(ONCRPCResponseHeader.ACCEPT_STAT_PROC_UNAVAIL,
                ErrNo.EINVAL,"requested operation ("+hdr.getTag()+") is not available on this "+name));
            return;
        } 
        
        if (op.canBeDisabled() && !startRequest()){
            rq.sendException(new errnoException(
                    org.xtreemfs.babudb.replication.operations.ErrNo.SERVICE_UNAVAILABLE,
                    "Replication is paused!",null));
            return;
        }
        
        Request rpcrq = new Request(rq);
        try {
            Serializable message = op.parseRPCMessage(rpcrq);
            if (message!=null) throw new Exception(message.getTypeName());
        } catch (Throwable ex) {
            rq.sendGarbageArgs(ex.toString(),new ProtocolException(ONCRPCResponseHeader.ACCEPT_STAT_SYSTEM_ERR, 
                ErrNo.EINVAL,"message could not be retrieved"));
            finishRequest(op.canBeDisabled());
            return;
        }
        
        try {
            op.startRequest(rpcrq);
        } catch (Throwable ex) {
            rq.sendInternalServerError(ex,new errnoException(ErrNo.ENOSYS,ex.getMessage(),null));
            finishRequest(op.canBeDisabled());
            return;
        }
        finishRequest(op.canBeDisabled());
    }
    
    /**
     * Increment an internal counter, if the request can be proceed.
     * 
     * @return true, if the request can be proceed, or false if the replication was stopped before.
     */
    private boolean startRequest() {
        synchronized (activeRequests) {
            if (stopped) return false;
            else activeRequests.incrementAndGet();
            return true;
        }
    }
    
    /**
     * Notifies the listener if available if there are no requests left.
     * 
     * @param canBeDisabled - if the operation of the request to finish can be disabled.
     */
    private void finishRequest(boolean canBeDisabled) {
        if (canBeDisabled) {
            synchronized (activeRequests) {
                if (activeRequests.decrementAndGet() == 0 && listener != null) {
                    listener.finished(null);
                    listener = null;
                }
            }
        }
    }

    /**
     * Simple security check by identifying the client.
     * 
     * @param clientIdentity
     * @return true if the client could be identified, false otherwise.
     */
    private boolean checkIdentity (SocketAddress clientIdentity) {
        if (clientIdentity instanceof InetSocketAddress) {
            return permittedClients.contains(((InetSocketAddress) clientIdentity).getAddress());
        }
        Logging.logMessage(Logging.LEVEL_ERROR, this, "Access-rights for client: '"+clientIdentity+"' could not be validated.");
        return false;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.include.foundation.LifeCycleListener#crashPerformed()
     */
    @Override
    public void crashPerformed() {
        Logging.logMessage(Logging.LEVEL_ERROR, this, "crashed... " +
        		"pauses replication!");
        pauses(null);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.include.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() {
        Logging.logMessage(Logging.LEVEL_NOTICE, this, "terminated successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.include.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() {
        Logging.logMessage(Logging.LEVEL_NOTICE, this, "started successfully.");
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
    
/*
 * State interface operations
 */
    /**
     * Needed to respond {@link stateRequest}s.
     * 
     * @return the {@link DispatcherState} of the {@link RequestDispatcher}, 
     *      including the {@link LSN} of the last locally written {@link LogEntry}.
     */
    public abstract DispatcherState getState();
    
    /**
     * Stops the dispatcher, by disabling all its replication features.
     * @param listener - can be null.
     */
    public void pauses(SimplifiedBabuDBRequestListener listener) {
        this.stopped = true;
        this.isMaster = false;
        if (listener!=null) {
            synchronized (activeRequests) {
                if (activeRequests.get() == 0) listener.finished(null);
                else this.listener = listener;
            }
        }
    }
    
    /**
     * Starts the dispatcher, by resetting all its replication features.
     * 
     * @param state
     * @throws BabuDBException 
     */
    public void continues(DispatcherState state) throws BabuDBException {
        if (!this.stopped) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, "The replication has to be stopped, before it can be continued!");
        this.stopped = false;
    }
    
    /**
     * State of the dispatcher after shutting it down.
     * 
     * @author flangner
     * @since 08/07/2009
     */
    public static class DispatcherState {
        
        public final LSN latest;
        public BlockingQueue<StageRequest> requestQueue;
        
        DispatcherState(LSN latest, BlockingQueue<StageRequest> backupQueue) {
            this.latest = latest;
            this.requestQueue = backupQueue;
        }
        
        DispatcherState(LSN latest) {
            this.latest = latest;
            this.requestQueue = null;
        }
    }
}
