/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetAddress;

import org.xtreemfs.foundation.LifeCycleListener;

/**
 * <p>
 * Interface for the replication user operations.
 * </p>
 * 
 * @author flangner
 * @since 09/14/2009
 */

public interface ReplicationManager extends LifeCycleListener {

    /**
     * <p>
     * Changes the database replication master. Uses this, if
     * your {@link BabuDBRequestListener} recognizes an failure due
     * the replication and want to help BabuDB to recognize it.
     * </p>
     */
    public void manualFailover();
    
    /**
     * <p>
     * Stops the replication process by shutting down the dispatcher.
     * And resetting the its state.
     * </p>
     */
    public void shutdown() throws Exception;

    /**
     * @return the currently designated master, or null, if BabuDB is 
     *         suspended at the moment.
     */
    public InetAddress getMaster();
    
    /**
     * @return true, if the replication is running in master mode. false, otherwise.
     */
    public boolean isMaster();
}