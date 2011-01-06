/*
 * Copyright (c) 2010, Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
 * 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this 
 * list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * Neither the name of the Konrad-Zuse-Zentrum fuer Informationstechnik Berlin 
 * nor the names of its contributors may be used to endorse or promote products 
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.transmission;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;

import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.replication.Coinable;
import org.xtreemfs.babudb.replication.Layer;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsVerification;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestDispatcher;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;

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
    Coinable<Map<Integer, Operation>, ParticipantsVerification>, 
    TransmissionToServiceInterface {
        
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
    public TransmissionLayer(ReplicationConfig config) throws IOException {
        this.fileIO = new FileIO(config);
        
        // ---------------------------------
        // initialize the RPCNIOSocketClient
        // ---------------------------------
        this.rpcClient = new RPCNIOSocketClient(config.getSSLOptions(), 
                ReplicationConfig.REQUEST_TIMEOUT,
                ReplicationConfig.CONNECTION_TIMEOUT);
        
        // ---------------------------------
        // initialize the RequestDispatcher
        // ---------------------------------
        this.dispatcher = new RequestDispatcher(config);
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
     * @see org.xtreemfs.babudb.replication.transmission.Coinable#
     * coinDispatcher(java.util.Map, org.xtreemfs.babudb.replication.service.
     * accounting.ParticipantsVerification)
     */
    @Override
    public void coin(Map<Integer, Operation> operations, 
            ParticipantsVerification verificator) {
        
        this.dispatcher.setOperations(operations);
        this.dispatcher.registerVerificator(verificator);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.ClientFactory#
     * getClient(java.net.SocketAddress)
     */
    @Override
    public PBRPCClientAdapter getClient(SocketAddress receiver) {
        assert (receiver instanceof InetSocketAddress);
        
        return new PBRPCClientAdapter(this.rpcClient, 
                (InetSocketAddress) receiver);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#
     * _setLifeCycleListener(org.xtreemfs.foundation.LifeCycleListener)
     */
    @Override
    public void _setLifeCycleListener(LifeCycleListener listener) {
        this.dispatcher.setLifeCycleListener(listener);
        this.rpcClient.setLifeCycleListener(listener);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.Layer#start()
     */
    @Override
    public void start() {
        try {
            this.dispatcher.start();
            this.dispatcher.waitForStartup();
            
            this.rpcClient.start();
            this.rpcClient.waitForStartup();
        } catch (Exception e) {
            this.listener.crashPerformed(e);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.Layer#asyncShutdown()
     */
    @Override
    public void asyncShutdown() {
        this.dispatcher.shutdown();
        this.rpcClient.shutdown();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.Layer#shutdown()
     */
    @Override
    public void shutdown() {
        try {    
            this.dispatcher.shutdown();
            this.dispatcher.waitForShutdown();
            
            this.rpcClient.shutdown();
            this.rpcClient.waitForShutdown();  
        } catch (Exception e) {
            this.listener.crashPerformed(e);
        }
    }
}
