/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.logic;

import org.xtreemfs.babudb.replication.service.ReplicationStage;
import org.xtreemfs.babudb.replication.service.SlaveView;
import org.xtreemfs.babudb.replication.service.ReplicationStage.ConnectionLostException;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;

/**
 * Interface for replication-behavior classes.
 * 
 * @author flangner
 * @since 06/08/2009
 */

public abstract class Logic {
    
    protected final ReplicationStage stage;
    
    protected final SlaveView        slaveView;
        
    protected final FileIOInterface  fileIO;
    
    public Logic(ReplicationStage stage, SlaveView slaveView, FileIOInterface fileIO) {
        
        this.slaveView = slaveView;
        this.fileIO = fileIO;
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
     * @throws Exception if an uncaught-able error occurred.
     */
    public abstract void run() throws ConnectionLostException, InterruptedException, Exception;
}