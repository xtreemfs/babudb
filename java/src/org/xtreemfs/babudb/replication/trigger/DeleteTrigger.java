/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.trigger;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.events.DeleteEvent;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.deleteRequest;

/**
 * Triggers the {@link DeleteEvent}.
 * 
 * @author flangner
 * @since 06/05/2009
 */

public class DeleteTrigger implements Trigger {

    private int eventNumber;
    private LSN lsn;
    private String db;
    private boolean delete;
    
    public DeleteTrigger() {
        this.eventNumber = new deleteRequest().getTag();
    }
   
    public DeleteTrigger(LSN lsn, String dbName, boolean toDelete) {
        this.eventNumber = new deleteRequest().getTag();
        this.lsn = lsn;
        this.db = dbName;
        this.delete = toDelete;
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
     * @return the delete
     */
    public boolean isDelete() {
        return delete;
    }
}