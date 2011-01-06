/*
 * Copyright (c) 2010-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.clients;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;

/**
 * Client to access the fundamental services of an participating server.
 *
 * @author flangner
 * @since 04/13/2010
 */
public interface ConditionClient extends ClientInterface {

    /**
     * The {@link LSN} of the latest written LogEntry.
     * 
     * @return the {@link ClientResponseFuture} receiving a state as {@link LSN}.
     */
    public ClientResponseFuture<LSN> state();

    /**
     * The local time-stamp of the registered participant.
     * 
     * @return the {@link ClientResponseFuture} receiving a time-stamp.
     */
    public ClientResponseFuture<Long> time();

    /**
     * Sends a {@link FleaseMessage} to the client.
     * 
     * @return the {@link ClientResponseFuture} as proxy for the response.
     */
    public ClientResponseFuture<?> flease(FleaseMessage message);

    /**
     * Updates the latest {@link LSN} of the slave at the master.
     * 
     * @param lsn
     * @return the {@link ClientResponseFuture}.
     */
    public ClientResponseFuture<?> heartbeat(LSN lsn);
}