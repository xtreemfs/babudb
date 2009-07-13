/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.trigger;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.events.CopyEvent;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.copyRequest;

/**
 * Triggers the {@link CopyEvent}.
 * 
 * @author flangner
 * @since 06/05/2009
 */

public class CopyTrigger implements Trigger {

    private int eventNumber;
    private LSN lsn;
    private String source;
    private String dest;
    
    public CopyTrigger() {
        this.eventNumber = new copyRequest().getOperationNumber();
    }
   
    public CopyTrigger(LSN lsn, String sourceDB, String destDB) {
        this.eventNumber = new copyRequest().getOperationNumber();
        this.lsn = lsn;
        this.source = sourceDB;
        this.dest = destDB;
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
     * @return the source
     */
    public String getSource() {
        return source;
    }

    /**
     * @return the dest
     */
    public String getDest() {
        return dest;
    }
}