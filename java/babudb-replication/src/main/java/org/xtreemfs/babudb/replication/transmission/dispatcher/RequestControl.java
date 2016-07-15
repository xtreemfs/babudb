/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission.dispatcher;

/**
 * Methods to control they way how requests will be handled on arrival.
 * 
 * @author flangner
 * @since 04/05/2011
 */
public interface RequestControl {

    /**
     * Method to prevent the handler from processing requests right away. Instead they will
     * be buffered and processed when the queue is drained.
     */
    public void enableQueuing();
    
    /**
     * Remove and process any of the requests that are at the queue. Queuing will be disabled 
     * afterwards.
     */
    public void processQueue();
}
