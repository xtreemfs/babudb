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
import org.xtreemfs.babudb.replication.LockableService;
import org.xtreemfs.babudb.replication.LockableService.ServiceLockedException;

/**
 * Interface between ControlLayer and {@link BabuDBInterface}.
 * 
 * @author flangner
 * @since 04/15/2010
 */
public interface ControlToBabuDBInterface {

    /**
     * @return the address of the current lease holder, or null if the lease is 
     *         not available at the moment.
     */
    public InetSocketAddress getLeaseHolder();
    
    /**
     * @param master - the address to compare with.
     * @return true, if the given master address is the address of this server. false otherwise.
     */
    public boolean amIMaster(InetSocketAddress master);
    
    /**
     * Method to register a {@link LockableService} to the control layer.
     * 
     * @param service
     */
    public abstract void registerUserInterface(LockableService service);
    
    /**
     * Method to register a {@link LockableService} to the control layer.
     * 
     * @param service
     */
    public abstract void registerReplicationInterface(LockableService service);
    
    /**
     * Completely locks all {@link BabuDB} services. They will throw {@link ServiceLockedException},
     * if accessed when locked.
     * Method blocks until a stable state has attuned, esp. there are no pending requests.
     * 
     * @throws InterruptedException
     */
    public abstract void lockAll() throws InterruptedException;
    
    /**
     * Unlocks all user accessible services.
     */
    public abstract void unlockUser();
    
    /**
     * Unlocks the replication related services.
     */
    public abstract void unlockReplication();
}