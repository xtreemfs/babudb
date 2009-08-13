/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.interfaces.Exceptions.ProtocolException;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCRequestHeader;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCResponseHeader;
import org.xtreemfs.babudb.interfaces.utils.Serializable;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.events.Event;
import org.xtreemfs.babudb.replication.events.EventResponse;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.operations.StateOperation;
import org.xtreemfs.babudb.replication.trigger.Trigger;
import org.xtreemfs.include.common.config.ReplicationConfig;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.pinky.SSLOptions;
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
    
    /** table of available internal events */
    protected final Map<Integer, Event>     events;
        
    /** outgoing requests */
    public final RPCNIOSocketClient         rpcClient;

    /** incoming operations */
    private final RPCNIOSocketServer        rpcServer;
    
    /** a list of permitted clients */
    protected final List<InetAddress>       permittedClients; 

    /** interface for babuDB core-components */
    public final BabuDB                 db;
    
/*
 * stages
 */
    
    /**
     * Initializing of the RequestDispatcher.
     * @param name
     * @param config
     * @param db
     * @throws IOException
     */
    public RequestDispatcher(String name, ReplicationConfig config, BabuDB db) throws IOException {
        this.name = name;
        this.db = db;
        this.permittedClients = new LinkedList<InetAddress>();
        
        // ---------------------
        // initialize operations
        // ---------------------
        
        operations = new HashMap<Integer, Operation>();
        Operation op = new StateOperation(this);
        operations.put(op.getProcedureId(), op);
        initializeOperations();

        // ---------------------
        // initialize events
        // ---------------------
        
        events = new HashMap<Integer, Event>();
        initializeEvents();
        
        // -------------------------------
        // initialize communication stages
        // -------------------------------
        
        SSLOptions serverSSLopts  = config.isUsingSSL() ? new SSLOptions(new FileInputStream(config.getServiceCredsFile()), config
                .getServiceCredsPassphrase(), config.getServiceCredsContainer(), new FileInputStream(
                config.getTrustedCertsFile()), config.getTrustedCertsPassphrase(), config
                .getTrustedCertsContainer(), false) : null;

        rpcServer = new RPCNIOSocketServer(config.getPort(), config.getAddress(), this, serverSSLopts);
        rpcServer.setLifeCycleListener(this);

        final SSLOptions clientSSLopts = config.isUsingSSL() ? new SSLOptions(new FileInputStream(config
                .getServiceCredsFile()), config.getServiceCredsPassphrase(), config
                .getServiceCredsContainer(), new FileInputStream(config.getTrustedCertsFile()), config
                .getTrustedCertsPassphrase(), config.getTrustedCertsContainer(), false) : null;


        rpcClient = new RPCNIOSocketClient(clientSSLopts, 5000, 5*60*1000);
        rpcClient.setLifeCycleListener(this);
        
        // -------------------------------
        // fill the permitted clients list
        // -------------------------------
        
        permittedClients.add(config.getMaster().getAddress());       
        for (InetSocketAddress slave : config.getSlaves()){
            permittedClients.add(slave.getAddress());
        }
    }

    /**
     * Register the events at the event's table. 
     */
    protected abstract void initializeEvents();

    /**
     * Register the events at the operation's table. 
     */
    protected abstract void initializeOperations();
        
    public void start() {    
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
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.include.foundation.oncrpc.server.RPCServerRequestListener#receiveRecord(org.xtreemfs.include.foundation.oncrpc.server.ONCRPCRequest)
     */
    @Override
    public void receiveRecord(ONCRPCRequest rq){
        if (!checkIdentity(rq.getClientIdentity())){
            rq.sendProtocolException(new ProtocolException(ONCRPCResponseHeader.ACCEPT_STAT_PROC_UNAVAIL,
                    ErrNo.EACCES,"you "+rq.getClientIdentity().toString()+" have no access rights to execute the requested operation on this "+name));
            return;
        }
                
        final ONCRPCRequestHeader hdr = rq.getRequestHeader();
        
        if (hdr.getInterfaceVersion() != ReplicationInterface.getVersion()) {
            rq.sendProtocolException(new ProtocolException(ONCRPCResponseHeader.ACCEPT_STAT_PROG_MISMATCH,
                    ErrNo.EINVAL,"invalid version requested"));
            return;
        }
        
        Operation op = operations.get(hdr.getOperationNumber());       
        if (op == null) {
            rq.sendProtocolException(new ProtocolException(ONCRPCResponseHeader.ACCEPT_STAT_PROC_UNAVAIL,
                ErrNo.EINVAL,"requested operation is not available on this "+name));
            return;
        } 
        
        Request rpcrq = new Request(rq);
        try {
            Serializable message = op.parseRPCMessage(rpcrq);
            if (message!=null) throw new Exception(message.getTypeName());
        } catch (Throwable ex) {
            rq.sendGarbageArgs(ex.toString());
            return;
        }
        
        try {
            op.startRequest(rpcrq);
        } catch (Throwable ex) {
            rq.sendInternalServerError(ex);
            return;
        }
    }
    
    /**
     * Receives internal requests - from a user or BabuDB itself for example.
     * 
     * @param trigger
     * @param Exception if something went horribly wrong.
     * 
     * @return the response object.
     */
    public EventResponse receiveEvent(Trigger trigger) throws NotEnoughAvailableSlavesException{
        return events.get(trigger.getEventNumber()).startEvent(trigger);
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
        Logging.logMessage(Logging.LEVEL_ERROR, this, "crashed... shutting down system!");
        shutdown();
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
     * @return the {@link LSN} of the last locally written {@link LogEntry}.
     */
    public LSN getLatestLSN() {
        return db.logger.getLatestLSN();
    }
}
