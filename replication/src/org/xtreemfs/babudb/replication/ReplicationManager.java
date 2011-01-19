/*
 * Copyright (c) 2009-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

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
     * Approach for a Worker to announce a new {@link LogEntry} 
     * <code>le</code> to the {@link ReplicationThread}.
     * </p>
     * 
     * @param le - the original {@link LogEntry}.
     * @param buffer - the serialized {@link LogEntry}.
     * 
     * @return the {@link ReplicateResponse}.
     */
    public ReplicateResponse replicate(LogEntry le, ReusableBuffer buffer);
    
    /**
     * @return a client to remotely access the BabuDB with master-privilege.
     */
    public RemoteAccessClient getRemoteAccessClient();
    
    /**
     * <p>
     * Registers the listener for a replicate call.
     * </p>
     * 
     * @param response
     */
    public void subscribeListener(ReplicateResponse response);
    
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
    public InetSocketAddress getMaster();
    
    /**
     * @return true, if the replication is running in master mode. 
     *         false, otherwise.
     */
    public boolean isMaster();
}