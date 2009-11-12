/*  Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
                    Felix Hupfeld, Felix Langner, Zuse Institute Berlin
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

    * Redistributions of source code must retain the above
      copyright notice, this list of conditions and the
      following disclaimer.
    * Redistributions in binary form must reproduce the above
      copyright notice, this list of conditions and the following
      disclaimer in the documentation and/or other materials
      provided with the distribution.
    * Neither the name of the Zuse Institute Berlin nor the
      names of its contributors may be used to endorse or promote
      products derived from this software without specific prior
      written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default return value for BabuDB requests.
 * 
 * @author flangner
 * @since 11/11/2009
 * @param <T>
 */
public class BabuDBRequest<T> implements BabuDBRequestResult<T>{
    
    private BabuDBRequestListener<T>  listener; 

    private T                   result;
    
    private BabuDBException     error; 
    
    private final AtomicBoolean finished = new AtomicBoolean(false);
    
    private final Object        context;
    
/*
 * constructors
 */
    
    /**
     * Creates a new future-object for a BabuDB request.
     */
    public BabuDBRequest() {
        this.context = null;
    }
    
    /**
     * Creates a new future-object for a BabuDB request.
     * 
     * @param context
     *          can be null.
     */
    public BabuDBRequest(Object context) {
        this.context = context;
    }
    
/*
 * state changing methods    
 */
    
    /**
     * Internal operation to run if the request was finished without 
     * a return value.
     * Has to be invoked exactly one time!
     */
    public void finished() {
        finished(null, null);
    }
    
    /**
     * Internal operation to run if the request was finished.
     * Has to be invoked exactly one time!
     * 
     * @param result
     */
    public void finished(T result) {
        finished(result, null);
    }
    
    /**
     * Internal operation to run if the request failed.
     * Has to be invoked exactly one time!
     * 
     * @param error 
     *            has to be not null, if an error occurs.
     */
    public void failed(BabuDBException error) {
        finished(null,error);
    }
    
    /**
     * <p>
     * This method resets the inner state of this request object,
     * to reuse it in the same context.
     * This helps to safe the performance of instantiating a new object. 
     * </p>
     */
    public void recycle() {
        synchronized (finished) {
            listener = null;
            error = null;
            result = null;
            finished.set(false);
        }
    }
    
/*
 * private methods    
 */
    
    /**
     * Internal operation to run if the request was finished.
     * Has to be invoked exactly one time!
     * 
     * @param result
     * @param error 
     *            has to be not null, if an error occurs.
     */
    private void finished(T result, BabuDBException error) {
        assert (result == null || error == null) : "Results are not permitted on error!";
        assert (!finished.get()) : "The request was already finished!";
        this.error = error;
        this.result = result;
        boolean check = finished.compareAndSet(false, true);
        assert (check) : "The request was already finished!";
        
        // notify the synchronously waiting instances
        synchronized (finished) {
            finished.notifyAll();
        }
        
        // notify the asynchronous-listener
        if (listener != null) {
            if (error != null)
                listener.failed(error,context);
            else
                listener.finished(result,context);
        }
    }

/*
 * BabuDBRequestResult interface
 */
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestResult#registerListener(org.xtreemfs.babudb.RequestListener)
     */
    public void registerListener(BabuDBRequestListener<T> listener) {
        synchronized (finished) {
            assert (this.listener == null) : "There is already a listener registered!";
            if (finished.get()) {
                if (error == null)
                    listener.finished(result,context);
                else
                    listener.failed(error,context);
            } else
                this.listener = listener;
        }
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestResult#get()
     */
    public T get() throws BabuDBException, InterruptedException {
        synchronized (finished) {
            if (!finished.get()) 
                finished.wait();            
        }
        
        if (error != null) throw error;
        return result;
    }
}
