/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.mock;

/**
 * @author flangner
 * @since 03/18/2011
 */
public interface Mock {
    
    public interface MockListener {
        
        public void eventHappened(String event);
        
        public void eventFailed(String event);
    }
    
    public void awaitEvent(MockListener listener, String event);
}