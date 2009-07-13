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
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.MasterRequestDispatcher;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.trigger.DeleteTrigger;
import org.xtreemfs.babudb.replication.trigger.Trigger;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponseAvailableListener;

/**
 * Broadcast operation to spread delete-calls from the master to the slaves.
 * 
 * @since 06/05/2009
 * @author flangner
 */

public class DeleteEvent extends Event {

    private final int procId;
    private final MasterRequestDispatcher dispatcher;
    
    public DeleteEvent(MasterRequestDispatcher dispatcher) {
        procId = new DeleteTrigger().getEventNumber();
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
    public EventResponse startEvent(Trigger trigger) throws NotEnoughAvailableSlavesException {
        assert(trigger!=null);
        assert(trigger instanceof DeleteTrigger);        
        DeleteTrigger t = (DeleteTrigger) trigger; 
        LSN lsn = t.getLsn();
        String db = t.getDb();
        boolean del = t.isDelete();
                
        List<SlaveClient> slaves = dispatcher.getSlavesForBroadCast(); 
        
        // setup the response
        final EventResponse result = new EventResponse(lsn,slaves.size()-dispatcher.getSyncN());
        dispatcher.subscribeListener(result);
        
        // perform the delete call at the clients
        for (final SlaveClient slave : slaves) {
            ((RPCResponse<Object>) slave.delete(lsn, db, del))
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
        
        return result;
    }
}