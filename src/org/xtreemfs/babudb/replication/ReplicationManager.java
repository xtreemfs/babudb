/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.LifeCycleListener;

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
     * Changes the database replication master. Uses this, if
     * your {@link BabuDBRequestListener} recognizes an failure due
     * the replication and want to help BabuDB to recognize it.
     * </p>
     */
    public abstract void manualFailover();
    
    /**
     * <p>
     * Stops the replication process by shutting down the dispatcher.
     * And resetting the its state.
     * </p>
     */
    public abstract void shutdown() throws Exception;

    /**
     * @return the currently designated master, or null, if BabuDB is 
     *         suspended at the moment.
     */
    public abstract InetSocketAddress getMaster();
    
    /**
     * @return true, if the replication is running in master mode. false, otherwise.
     */
    public boolean isMaster();
}