/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.babudb.replication.stages.ReplicationStage.ConnectionLostException;

/**
 * Interface for replication-behavior classes.
 * 
 * @author flangner
 * @since 06/08/2009
 */

public abstract class Logic {
    
    protected ReplicationStage stage;
    
    public Logic(ReplicationStage stage) {
        this.stage = stage;
    }
    
    /**
     * @return unique id, identifying the logic.
     */
    public abstract LogicID getId();
    
    /**
     * Function to execute, if logic is needed.
     * 
     * @throws ConnectionLostException if the connection to the participant is lost.
     * @throws InterruptedException if the execution was interrupted.
     */
    public abstract void run() throws ConnectionLostException, InterruptedException;
}