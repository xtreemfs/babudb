/*
 * Copyright (c) 2010-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication;

import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.LifeCycleThread;

/**
 * <p>
 * Abstraction of the transmission facilities used for the replication.
 * Includes interfaces for the layer above.
 * </p>
 *
 * @author flangner
 * @since 04/13/2010
 */
public abstract class Layer {

    /** listener for life cycle events */
    protected LifeCycleListener             listener = null;
    
    /**
     * Sets the given {@link LifeCycleListener} for all {@link LifeCycleThread}s
     * of this {@link Layer}.
     * 
     * @param listener - the {@link LifeCycleListener}.
     */
    public synchronized void setLifeCycleListener(LifeCycleListener listener) {
        assert (listener != null);
        
        if (this.listener == null)
            this.listener = listener;
    }
    
    /**
     * Sets the given {@link LifeCycleListener} for all {@link LifeCycleThread}s
     * of this {@link Layer}.
     * <b>-internal method-</b>
     * Use setLifeCycleListener instead!
     * 
     * @param listener - the {@link LifeCycleListener}.
     */
    public abstract void _setLifeCycleListener(LifeCycleListener listener);
    
    /**
     * Start the services embedded into this layer.
     */
    public abstract void start();

    /**
     * Shutdown the services of this layer without waiting until they are not 
     * running anymore.
     */
    public abstract void asyncShutdown();

    /**
     * Initializes shutdown sequence, synchronously waiting until its finished.
     */
    public abstract void shutdown();
}