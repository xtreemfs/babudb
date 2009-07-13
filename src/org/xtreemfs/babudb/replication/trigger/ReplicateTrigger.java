/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.trigger;

import java.io.IOException;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.BabuDBRequestListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateRequest;
import org.xtreemfs.babudb.replication.events.ReplicateEvent;
import org.xtreemfs.include.common.buffer.ReusableBuffer;

/**
 * Triggers the {@link ReplicateEvent}.
 * 
 * @author flangner
 * @since 06/05/2009
 */

public class ReplicateTrigger implements Trigger {

    private int eventNumber;
    private LSN lsn;
    private ReusableBuffer payload;
    private BabuDBRequestListener listener;
    private Object context;
    
    public ReplicateTrigger() {
        this.eventNumber = new replicateRequest().getOperationNumber();
    }
   
    public ReplicateTrigger(org.xtreemfs.babudb.log.LogEntry le, BabuDBRequestListener listener, Object context, Checksum checksum) throws IOException {
        this.eventNumber = new replicateRequest().getOperationNumber();
        this.context = context;
        this.listener = listener;
        this.lsn = le.getLSN();
        this.payload = le.serialize(checksum);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.trigger.Trigger#getEventNumber()
     */
    @Override
    public int getEventNumber() {
        return eventNumber;
    }

    /**
     * @return the lsn
     */
    public LSN getLsn() {
        return lsn;
    }

    /**
     * @return the payload
     */
    public ReusableBuffer getPayload() {
        return payload;
    }

    /**
     * @return the listener
     */
    public BabuDBRequestListener getListener() {
        return listener;
    }

    /**
     * @return the context
     */
    public Object getContext() {
        return context;
    }
}