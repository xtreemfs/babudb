/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages;

import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.SlaveRequestDispatcher;
import org.xtreemfs.include.foundation.LifeCycleThread;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponseAvailableListener;

/**
 * <p>Simple Thread for sending acknowledged {@link LSN}s to the master.</p>
 * 
 * @since 06/08/2009
 * @author flangner
 */

public class HeartbeatThread extends LifeCycleThread {
    
    /** 10 seconds */
    public final static long MAX_DELAY_BETWEEN_HEARTBEATS = 10*1000; 
    
    private final SlaveRequestDispatcher dispatcher;
    
    /** Holds the identifier of the last written LogEntry. */
    private volatile LSN latestLSN;
       
    /** set to true, if thread should shut down */
    private volatile boolean quit = false;
    
    /** set to true, if the thread should be stopped temporarily */
    private final AtomicBoolean halted = new AtomicBoolean(false);
    
    public HeartbeatThread(SlaveRequestDispatcher dispatcher, LSN initial) {
        super("HeartbeatThread");
        this.dispatcher = dispatcher;
        setLifeCycleListener(dispatcher);
        this.latestLSN =initial;
    }
    
    /**
     * <p>Sets {@code lsn} if it is greater than the available {@link LSN}.
     * If so, than a heartBeat is proceeded immediately.</p>
     * 
     * @param lsn
     */
    public synchronized void updateLSN(LSN lsn) {
        if (latestLSN.compareTo(lsn)<0) {
            latestLSN = lsn;
            synchronized (halted) {
                if (halted.compareAndSet(true, false)) halted.notify();
                else interrupt();
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Thread#start()
     */
    @Override
    public synchronized void start() {
        quit = false;
        super.start();
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        notifyStarted();
        
        while (!quit) {
            processHeartbeat();
            
            try {
                sleep(MAX_DELAY_BETWEEN_HEARTBEATS);
            } catch(InterruptedException e) { 
                /* ignored */
            }
            
            synchronized (halted) {
                try {
                    if (halted.get())
                        halted.wait();
                } catch (InterruptedException e) {
                    assert (quit==true) : "halted must be notified, if thread should not shut down";
                }
            }
        }
        
        notifyStopped();
    }

    /**
     * Sends a heartBeat message to the master.
     */
    @SuppressWarnings("unchecked")
    private void processHeartbeat() {
        dispatcher.master.heartbeat(latestLSN).registerListener(new RPCResponseAvailableListener() {
        
            @Override
            public void responseAvailable(RPCResponse r) { if (r!=null) r.freeBuffers(); }
        });
    }

    /**
     * <p>Stops the {@link HeartbeatThread} temporarily,
     * until the next update is received.</p>
     */
    public void infarction() {
        synchronized (halted) {
            if (halted.compareAndSet(false, true)) interrupt();
        }
    }
    
    /**
     * shut the thread down
     */
    public void shutdown() {
        if (quit != true) {
            this.quit = true;
            this.interrupt();
        }
    }
}