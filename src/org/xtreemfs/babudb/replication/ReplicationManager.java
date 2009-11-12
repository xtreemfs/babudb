/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCException;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.include.foundation.LifeCycleListener;

/**
 * <p>
 * Interface for the replication user operations.
 * </p>
 * 
 * @author flangner
 * @since 09/14/2009
 */

public interface ReplicationManager extends LifeCycleListener{

    /**
     * <p>Performs a network broadcast to get the latest LSN from every available DB.</p>
     * 
     * @param babuDBs
     * @return the LSNs of the latest written LogEntries for all given <code>babuDBs</code>.
     */
    public abstract Map<InetSocketAddress, LSN> getStates(
            List<InetSocketAddress> babuDBs);

    /**
     * <p>
     * Makes the locally BabuDB running in slave-mode.
     * </p>
     * 
     * @param master - address of the new declared master.
     * @throws InterruptedException
     * @throws BabuDBException
     */
    public abstract void declareToSlave(InetSocketAddress master)
            throws InterruptedException;

    /**
     * <p>
     * Makes the locally BabuDB running in master-mode.
     * </p>
     * <p>
     * Stops all declared slaves and synchronizes the local BabuDB 
     * with the latest slave.
     * Then restarts all participants in slave mode 
     * </p>
     * 
     * @param conf
     * @throws NotEnoughAvailableSlavesException 
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ONCRPCException 
     * @throws BabuDBException - if checkpoint could not be established.
     */
    public abstract void declareToMaster()
            throws NotEnoughAvailableSlavesException, IOException,
            ONCRPCException, InterruptedException, BabuDBException;

    /**
     * <p>
     * Sets a flag to stop the replication. Uses this, if
     * your {@link BabuDBRequestListener} recognizes an failure due
     * the replication and want to use an external fail-over mechanism.
     * </p>
     */
    public abstract void halt();
    
    /**
     * <p>
     * Stops the replication process by shutting down the dispatcher.
     * And resetting the its state.
     * </p>
     */
    public abstract void shutdown();

    /**
     * @return the currently designated master, or null, if unknown.
     */
    public abstract InetSocketAddress getMaster();
    
    /**
     * @return true, if the replication is running in master mode. false, otherwise.
     */
    public boolean isMaster();
}