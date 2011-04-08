/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import static org.junit.Assert.*;
import static org.xtreemfs.babudb.replication.TestParameters.*;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.mock.BabuDBMock;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.LockableService;
import org.xtreemfs.babudb.replication.LockableService.ServiceLockedException;
import org.xtreemfs.babudb.replication.control.ControlLayerInterface;
import org.xtreemfs.babudb.replication.service.ReplicationRequestHandler;
import org.xtreemfs.babudb.replication.service.RequestManagement;
import org.xtreemfs.babudb.replication.service.StageRequest;
import org.xtreemfs.babudb.replication.service.ReplicationStage.BusyServerException;
import org.xtreemfs.babudb.replication.service.accounting.StatesManipulation;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates.UnknownParticipantException;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;
import org.xtreemfs.babudb.replication.transmission.FileIO;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestControl;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestDispatcher;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.logging.Logging.Category;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;

/**
 * Test of the operation logic for replication requests.
 * 
 * @author flangner
 * @since 04/08/2011
 */
public class ReplicationOperationsTest implements LifeCycleListener {
    
    private static ReplicationConfig config;
    private RequestDispatcher dispatcher;
    private RPCNIOSocketClient client;
    
    // test data
    private final AtomicReference<LSN> lastOnView = new AtomicReference<LSN>(new LSN(1,1L));
    
    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Logging.start(Logging.LEVEL_ERROR, Category.all);
        TimeSync.initializeLocal(TIMESYNC_GLOBAL, TIMESYNC_LOCAL);
        
        config = new ReplicationConfig("config/replication_server0.test", conf0);
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TimeSync ts = TimeSync.getInstance();
        ts.shutdown();
        ts.waitForShutdown();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        
        client = new RPCNIOSocketClient(config.getSSLOptions(), RQ_TIMEOUT, CON_TIMEOUT);
        client.start();
        client.waitForStartup();
        
        dispatcher = new RequestDispatcher(config);
        dispatcher.setLifeCycleListener(this);
        dispatcher.addHandler(
                new ReplicationRequestHandler(new StatesManipulation() {
                    
                    @Override
                    public void update(InetSocketAddress participant, LSN acknowledgedLSN, long receiveTime)
                            throws UnknownParticipantException {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void requestFinished(SlaveClient slave) {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void markAsDead(SlaveClient slave) {
                        // TODO Auto-generated method stub
                        
                    }
                }, new ControlLayerInterface() {
                    
                    @Override
                    public void updateLeaseHolder(InetSocketAddress leaseholder) throws Exception {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void receive(FleaseMessage message) {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void driftDetected() {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void unlockUser() {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void unlockReplication() {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void registerUserInterface(LockableService service) {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void registerReplicationControl(LockableService service) {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void registerProxyRequestControl(RequestControl control) {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void notifyForSuccessfulFailover(InetSocketAddress master) {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void lockAll() throws InterruptedException {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public boolean isItMe(InetSocketAddress address) {
                        // TODO Auto-generated method stub
                        return false;
                    }
                    
                    @Override
                    public InetSocketAddress getLeaseHolder() {
                        // TODO Auto-generated method stub
                        return null;
                    }
                }, new BabuDBInterface(new BabuDBMock("BabuDBMock", conf0)), new RequestManagement() {
                    
                    @Override
                    public void finalizeRequest(StageRequest op) {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void enqueueOperation(Object[] args) throws BusyServerException, ServiceLockedException {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void createStableState(LSN lastOnView, InetSocketAddress master) {
                        // TODO Auto-generated method stub
                        
                    }
                }, lastOnView, config.getChunkSize(), new FileIO(config), MAX_Q));
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        client.shutdown();
        client.waitForShutdown();
        
        dispatcher.shutdown();
        dispatcher.waitForShutdown();
    }

    /**
     * Test method for {@link org.xtreemfs.babudb.replication.service.operations.LocalTimeOperation#processRequest(org.xtreemfs.babudb.replication.transmission.dispatcher.Request)}.
     */
    @Test
    public void testProcessRequest() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#parseRPCMessage(org.xtreemfs.babudb.replication.transmission.dispatcher.Request)}.
     */
    @Test
    public void testParseRPCMessage() {
        fail("Not yet implemented");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() { }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() { }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#crashPerformed(java.lang.Throwable)
     */
    @Override
    public void crashPerformed(Throwable cause) {
        fail("Dispatcher crashed: " + cause.getMessage());
    }
}
