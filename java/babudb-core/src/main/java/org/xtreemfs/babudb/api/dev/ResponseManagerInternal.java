/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev;

import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.foundation.LifeCycleThread;

/**
 * Thread to process response handles for BabuDB request futures. This is necessary to decouple 
 * internal BabuDB threads from user listeners. It does not prevent user listeners from deadlocking
 * them selves
 * 
 * @author flangner
 * @since 05/29/2011
 */
public abstract class ResponseManagerInternal extends LifeCycleThread {

    /**
     * Default constructor to preinitialize a ResponseManager object.
     */
    public ResponseManagerInternal() {
        super("RspMan");
    }

    /**
     * This method enqueues a listener with the results that have to be passed to it.
     * 
     * @param <T>
     * @param listener
     * @param error
     * @param result
     * @param context
     * @throws InterruptedException
     */
    public abstract <T> void enqueueResponse(DatabaseRequestListener<T> listener, 
            BabuDBException error, T result, Object context) throws InterruptedException;
}
