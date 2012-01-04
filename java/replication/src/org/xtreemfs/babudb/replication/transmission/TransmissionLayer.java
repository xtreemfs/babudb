/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.transmission;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.replication.Layer;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.proxy.DatabaseManagerProxy;
import org.xtreemfs.babudb.replication.proxy.ProxyAccessClient;
import org.xtreemfs.babudb.replication.transmission.client.ClientFactory;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter;
import org.xtreemfs.babudb.replication.transmission.client.ProxyAccessClientAdapter;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestDispatcher;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.Auth;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.AuthType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.UserCredentials;

/**
 * <p>
 * Abstraction of the transmission facilities used for the replication.
 * Includes interfaces for the layer above. Implements {@link Layer}.
 * </p>
 * 
 * @author flangner
 * @since 04/12/2010
 */
public class TransmissionLayer extends Layer implements ClientFactory, 
        TransmissionToServiceInterface {
        
    public final static AuthType AUTH_TYPE = AuthType.AUTH_NONE;
    public final static String USER = ReplicationManager.VERSION;
    
    public final static Auth AUTHENTICATION = 
        Auth.newBuilder().setAuthType(AUTH_TYPE).build();
    
    public final static UserCredentials USER_CREDENTIALS = 
        UserCredentials.newBuilder().setUsername(USER).build();
    
    /** low level client for outgoing RPCs */
    private final RPCNIOSocketClient    rpcClient;
    
    /** dispatcher to process incoming RPCs */
    private final RequestDispatcher     dispatcher;
        
    /** interface for accessing files defined by BabuDB */
    private final FileIO                fileIO;
    
    /**
     * @param config
     * 
     * @throws IOException if the {@link RPCNIOSocketClient} could not be 
     *                     started.
     */
    public TransmissionLayer(ReplicationConfig config) 
            throws IOException {
        
        fileIO = new FileIO(config);
        
        // ---------------------------------
        // initialize the RPCNIOSocketClient
        // ---------------------------------
        InetSocketAddress bindPoint = config.getAddress() != null ? new InetSocketAddress(config.getAddress(), 0)
                : null;
        rpcClient = new RPCNIOSocketClient(config.getSSLOptions(), ReplicationConfig.REQUEST_TIMEOUT,
                ReplicationConfig.CONNECTION_TIMEOUT, -1, -1, bindPoint);
        
        // ---------------------------------
        // initialize the RequestDispatcher
        // ---------------------------------
        dispatcher = new RequestDispatcher(config);
    }
    
/*
 * Overridden methods
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.
     * TransmissionToServiceInterface#getFileIOInterface()
     */
    @Override
    public FileIOInterface getFileIOInterface() {
        return this.fileIO;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.ClientFactory#getClient(
     *      java.net.InetSocketAddress)
     */
    @Override
    public ReplicationClientAdapter getClient(InetSocketAddress receiver) {
        
        return new ReplicationClientAdapter(rpcClient, receiver);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.client.ClientFactory#getProxyClient(
     *          org.xtreemfs.babudb.replication.proxy.DatabaseManagerProxy)
     */
    @Override
    public ProxyAccessClient getProxyClient(DatabaseManagerProxy dbManProxy) {
        return new ProxyAccessClientAdapter(rpcClient, dbManProxy);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#
     *          _setLifeCycleListener(org.xtreemfs.foundation.LifeCycleListener)
     */
    @Override
    public void _setLifeCycleListener(LifeCycleListener listener) {
        dispatcher.setLifeCycleListener(listener);
        rpcClient.setLifeCycleListener(listener);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.Layer#start()
     */
    @Override
    public void start() {
        try {
            dispatcher.start();
            dispatcher.waitForStartup();
            
            rpcClient.start();
            rpcClient.waitForStartup();
        } catch (Exception e) {
            listener.crashPerformed(e);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.Layer#asyncShutdown()
     */
    @Override
    public void asyncShutdown() {
        dispatcher.shutdown();
        rpcClient.shutdown();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.Layer#shutdown()
     */
    @Override
    public void shutdown() {
        try {    
            dispatcher.shutdown();
            dispatcher.waitForShutdown();
            
            rpcClient.shutdown();
            rpcClient.waitForShutdown();  
        } catch (Exception e) {
            listener.crashPerformed(e);
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.TransmissionToServiceInterface#addRequestHandler(org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler)
     */
    @Override
    public void addRequestHandler(RequestHandler handler) {
        dispatcher.addHandler(handler);
    }
}
