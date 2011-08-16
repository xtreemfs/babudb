/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.control;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.FleaseMessageReceiver;
import org.xtreemfs.babudb.replication.LockableService;
import org.xtreemfs.babudb.replication.LockableService.ServiceLockedException;
import org.xtreemfs.babudb.replication.control.TimeDriftDetector.TimeDriftListener;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestControl;

/**
 * Interface between ControlLayer and {@link BabuDBInterface}.
 * 
 * @author flangner
 * @since 04/15/2010
 */
public interface ControlLayerInterface extends TimeDriftListener, FleaseMessageReceiver, 
        FleaseEventListener{

    /**
     * Waits until a lease holder becomes available if necessary.
     * @param timeout - in ms to wait for a lease holder to become available. 
     *                  May be 0 to wait forever.
     * 
     * @return the address of the current lease holder.
     */
    public InetSocketAddress getLeaseHolder(int timeout) throws InterruptedException;
    
    /**
     * @param address - the address to compare with.
     * @return true, if the given address is the address of this server. false otherwise.
     */
    public boolean isItMe(InetSocketAddress address);
    
    /**
     * Method to register a {@link LockableService} to the control layer.
     * 
     * @param service
     */
    public void registerUserInterface(LockableService service);
    
    /**
     * Use only at initialization. Waits for the first failover to happen.
     * 
     * @throws InterruptedException if waiting was interrupted.
     */
    public void waitForInitialFailover() throws InterruptedException;
    
    /**
     * Method to register a {@link LockableService} to the control layer.
     * 
     * @param service
     */
    public void registerReplicationControl(LockableService service);
    
    /**
     * Method to register the proxy {@link RequestControl} to the control layer;
     * 
     * @param control
     */
    public void registerProxyRequestControl(RequestControl control);
    
    /**
     * Completely locks all {@link BabuDB} services. They will throw {@link ServiceLockedException},
     * if accessed when locked.
     * Method blocks until a stable state has attuned, esp. there are no pending requests.
     * 
     * @throws InterruptedException
     */
    public void lockAll() throws InterruptedException;
    
    /**
     * Unlocks all user accessible services.
     */
    public void unlockUser();
    
    /**
     * Method to notify about a successfully processed failover request triggered by master.
     * 
     * @param master
     */
    public void notifyForSuccessfulFailover(InetSocketAddress master);
        
    /**
     * Unlocks the replication related services.
     */
    public void unlockReplication();
}