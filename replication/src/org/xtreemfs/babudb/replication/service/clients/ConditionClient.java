/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.clients;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Timestamp;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;

/**
 * Client to access the fundamental services of an participating server.
 *
 * @author flangner
 * @since 04/13/2010
 */
public interface ConditionClient extends ClientInterface {

    /**
     * The {@link LSN} of the latest written LogEntry. The client is locked to preserve this state
     * until the next master call.
     * 
     * @return the {@link ClientResponseFuture} receiving a state as {@link LSN}.
     */
    public ClientResponseFuture<LSN, org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN> state();
    
    /**
     * The {@link LSN} of the latest written LogEntry. Might have become incremented while receiving
     * the answer of this call. 
     * 
     * @return the {@link ClientResponseFuture} receiving a state as {@link LSN}.
     */
    public ClientResponseFuture<LSN, org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN> volatileState();

    /**
     * The local time-stamp of the registered participant.
     * 
     * @return the {@link ClientResponseFuture} receiving a time-stamp.
     */
    public ClientResponseFuture<Long,Timestamp> time();

    /**
     * Sends a {@link FleaseMessage} to the client.
     * 
     * @return the {@link ClientResponseFuture} as proxy for the response.
     */
    public ClientResponseFuture<Object, ErrorCodeResponse> flease(FleaseMessage message);

    /**
     * Updates the latest {@link LSN} of the slave at the master.
     * 
     * @param lsn
     * @param localPort
     * @return the {@link ClientResponseFuture}.
     */
    public ClientResponseFuture<Object, ErrorCodeResponse> heartbeat(LSN lsn, int localPort);
    
    /**
     * Forces the receiver to synchronize to the state given by lsn and make a checkpoint. Last step
     * of the master failover mechanism. 
     * 
     * @param lsn - to synchronize to.
     * @param localPort
     * @return the {@link ClientResponseFuture}.
     */
    public ClientResponseFuture<Object, ErrorCodeResponse> synchronize(LSN lsn, int localPort);
}