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
import org.xtreemfs.babudb.replication.LockableService.ServiceLockedException;
import org.xtreemfs.babudb.replication.service.ReplicationStage.BusyServerException;

/**
 * Request-flow controlling methods of the {@link ReplicationStage}.
 * 
 * @author flangner
 * @since 04/14/2010
 */
public interface RequestManagement {

    /**
     * <p>Frees the given operation and decrements the number of requests.</p>
     * 
     * @param op
     */
    public void finalizeRequest(StageRequest op);

    /**
     * send an request for a stage operation
     *
     * @param rq
     *            the request
     * @param the
     *            method in the stage to execute
     *            
     * @throws BusyServerException
     * @throws ServiceLockedException;
     */
    public void enqueueOperation(Object[] args) throws BusyServerException, ServiceLockedException;

    /**
     * This method is used to synchronize a server with the master to establish a global stable 
     * state.
     * 
     * @param lastOnView
     * @param master
     */
    public void createStableState(LSN lastOnView, InetSocketAddress master);
}