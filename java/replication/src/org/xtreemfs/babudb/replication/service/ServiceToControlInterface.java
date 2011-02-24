/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service;

import java.io.IOException;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.replication.control.ControlLayer;
import org.xtreemfs.babudb.replication.service.accounting.LatestLSNUpdateListener;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;

/**
 * The interface for the {@link ControlLayer} to access methods of the 
 * {@link ServiceLayer}.
 *
 * @author flangner
 * @since 04/14/2010
 */
public interface ServiceToControlInterface {

    /**
     * Replicate the given LogEntry.
     * 
     * @param le - the original {@link LogEntry}.
     * 
     * @return the {@link ReplicateResponse}.
     */
    public ReplicateResponse replicate(LogEntry le);

    /**
     * Tries to synchronize with one of the most up-to-date replication participants and assures a 
     * stable state after log-file switch for at least N servers.
     * 
     * @param listener to handle the result of the synchronization progress.
     * 
     * @throws BabuDBException if synchronization failed. 
     * @throws InterruptedException if execution was interrupted.
     * @throws IOException if the log-file could not be switched.
     */
    public void synchronize(SyncListener listener) throws BabuDBException, InterruptedException, 
            IOException;

    /**
     * Resets pending requests at the {@link ServiceLayer}. Listeners of all pending requests on the 
     * {@link ParticipantsStates} table have been marked as failed after this call.
     */
    public void reset();
    
    /**
     * @return an overview for available participants.
     */
    public ParticipantsOverview getParticipantOverview();

    /**
     * Registers the listener for a replicate call.
     * 
     * @param listener
     */
    public void subscribeListener(LatestLSNUpdateListener listener);
}