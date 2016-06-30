/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.control.ControlLayerInterface;
import org.xtreemfs.babudb.replication.service.ReplicationStage.BusyServerException;

/**
 * Request-flow controlling methods of the {@link ReplicationStage}.
 * 
 * @author flangner
 * @since 04/14/2010
 */
public interface RequestManagement {

    /**
     * Method to delegate dispatched replicate-requests from the replication logic itself.
     *
     * @param rq - the request
     *            
     * @throws BusyServerException if queue-size has reached MAX_Q.
     */
    public void enqueueOperation(Object[] rq) throws BusyServerException;

    /**
     * This method is used to synchronize a server with the master to establish a global stable 
     * state.
     * 
     * @param lastOnView
     * @param master
     * @param control
     * 
     * @throws InterruptedException if creation of a stable state was interrupted by shutdown 
     *                              or crash.
     */
    public void createStableState(LSN lastOnView, InetSocketAddress master, ControlLayerInterface control) 
            throws InterruptedException;
}