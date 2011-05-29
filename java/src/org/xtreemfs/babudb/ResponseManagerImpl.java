/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.dev.ResponseManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;

/**
 * Thread to process response handles for BabuDB request futures. This is necessary to decouple 
 * internal BabuDB threads from user listeners. It does not prevent user listeners from deadlocking
 * them selves
 * 
 * @author flangner
 * @since 05/29/2011
 */
public class ResponseManagerImpl extends ResponseManagerInternal {

    private final BlockingQueue<ResponseRecord<?>>      queue;
    
    private boolean                                     quit = true;
    
    /**
     * @param max_Q - max length of the queue.
     */
    public ResponseManagerImpl(int max_Q) {
        super();
        if (max_Q > 0) {
            queue = new LinkedBlockingQueue<ResponseRecord<?>>(max_Q);
        } else {
            queue = new LinkedBlockingQueue<ResponseRecord<?>>();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.ResponseManagerInternal#enqueueResponse(
     *          org.xtreemfs.babudb.api.database.DatabaseRequestListener, 
     *          org.xtreemfs.babudb.api.exception.BabuDBException, java.lang.Object, 
     *          java.lang.Object)
     */
    @Override
    public <T> void enqueueResponse(DatabaseRequestListener<T> listener, BabuDBException error, 
            T result, Object context) throws InterruptedException {
        
        assert (result == null || error == null && result != error);
        
        queue.put(new ResponseRecord<T>(listener, error, result, context));
    }
    
    /* (non-Javadoc)
     * @see java.lang.Thread#start()
     */
    @Override
    public synchronized void start() {
        quit = false;
        super.start();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleThread#shutdown()
     */
    @Override
    public synchronized void shutdown() throws Exception {
        quit = true;
        interrupt();
    }
    
    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void run() {
        
        ResponseRecord respRec = null;
        notifyStarted();
        
        while (!quit) {
            try {
                respRec = queue.take();
                
                if (respRec.error == null) {
                    respRec.listener.finished(respRec.result, respRec.context);
                } else {
                    respRec.listener.failed(respRec.error, respRec.context);
                }
            } catch (InterruptedException e) {
                if (!quit) {
                    notifyCrashed(e);
                }
            }
        }
        
        notifyStopped();
    }
    
    /**
     * Data record for processing the listener.
     * 
     * @author flangner
     * @since 05/29/2011
     * @param <T>
     */
    private final static class ResponseRecord<T> {
        private final DatabaseRequestListener<T>        listener;
        private final BabuDBException                   error;
        private final Object                            context;
        private final T                                 result;
        
        private ResponseRecord(DatabaseRequestListener<T> listener, BabuDBException error, T result,
                Object context) {
            
            this.listener = listener;
            this.error = error;
            this.result = result;
            this.context = context;
        }
    }
}
