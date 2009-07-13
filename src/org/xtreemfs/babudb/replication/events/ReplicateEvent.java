/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.events;

import java.util.List;

import org.xtreemfs.babudb.clients.SlaveClient;
import org.xtreemfs.babudb.interfaces.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.MasterRequestDispatcher;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.trigger.ReplicateTrigger;
import org.xtreemfs.babudb.replication.trigger.Trigger;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponseAvailableListener;

/**
 * Broadcast operation to spread {@link LogEntry}s from the master to the slaves.
 * 
 * @since 06/05/2009
 * @author flangner
 */

public class ReplicateEvent extends Event {

    private final int procId;
    private final MasterRequestDispatcher dispatcher;
    
    public ReplicateEvent(MasterRequestDispatcher dispatcher) {
        procId = new ReplicateTrigger().getEventNumber();
        this.dispatcher = dispatcher;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.events.Event#getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return procId;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.events.Event#startEvent(org.xtreemfs.babudb.replication.trigger.Trigger)
     */
    @SuppressWarnings("unchecked")
    @Override
    public EventResponse startEvent(Trigger trigger) throws NotEnoughAvailableSlavesException{
        assert(trigger!=null);
        assert(trigger instanceof ReplicateTrigger);
        
        final ReplicateTrigger t = (ReplicateTrigger) trigger;  
        LSN lsn = t.getLsn();
        final ReusableBuffer buffer = t.getPayload();
        
        List<SlaveClient> slaves = dispatcher.getSlavesForBroadCast(); 
        
        // setup the response
        final EventResponse result = new EventResponse(lsn,slaves.size()-dispatcher.getSyncN()) {
            @Override
            public void upToDate() {
                super.upToDate();
                t.getListener().insertFinished(t.getContext());
            }
        };
        dispatcher.subscribeListener(result);
        
        // make the replicate call at the clients
        if (slaves.size() == 0) { 
            Logging.logMessage(Logging.LEVEL_ERROR, dispatcher, "There are no slaves available anymore! BabuDB runs if it would be in non-replicated mode.");
            if (buffer!=null) BufferPool.free(buffer); // FIXME : just for testing: replication with no slave is no replication...
        } else {
            for (final SlaveClient slave : slaves) {
                ((RPCResponse<Object>) slave.replicate(lsn, buffer))
                    .registerListener(new RPCResponseAvailableListener<Object>() {
                
                    @Override
                    public void responseAvailable(RPCResponse<Object> r) {
                        // evaluate the response
                        try {
                            r.get();
                            dispatcher.markSlaveAsFinished(slave);
                        } catch (Exception e) {
                            result.decrementPermittedFailures();
                            dispatcher.markSlaveAsDead(slave);
                        }
                        r.freeBuffers();
                    }
                });
            }
        }
        
        return result;
    }
}