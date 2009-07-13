/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.trigger;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.events.CreateEvent;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.createRequest;

/**
 * Triggers the {@link CreateEvent}.
 * 
 * @author flangner
 * @since 06/05/2009
 */

public class CreateTrigger implements Trigger {

    private int eventNumber;
    private LSN lsn;
    private String db;
    private int indices;
    
    public CreateTrigger() {
        this.eventNumber = new createRequest().getOperationNumber();
    }
   
    public CreateTrigger(LSN lsn, String dbName, int numIndices) {
        this.eventNumber = new createRequest().getOperationNumber();
        this.lsn = lsn;
        this.db = dbName;
        this.indices = numIndices;
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
     * @return the db
     */
    public String getDb() {
        return db;
    }

    /**
     * @return the indices
     */
    public int getIndices() {
        return indices;
    }
}