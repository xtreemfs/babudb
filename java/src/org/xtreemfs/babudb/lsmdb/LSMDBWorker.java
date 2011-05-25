/*
 * Copyright (c) 2008 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.lsmdb;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.database.UserDefinedLookup;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.logging.Logging;

/**
 * @author bjko
 */
public class LSMDBWorker extends LifeCycleThread {
    
    public static enum RequestOperation {
        INSERT, LOOKUP, PREFIX_LOOKUP, RANGE_LOOKUP, USER_DEFINED_LOOKUP, LOCK
    };
    
    private final AtomicBoolean                  locked = new AtomicBoolean(false);
    
    private final BabuDBInternal                 dbs;
    
    private final LinkedList<LSMDBRequest<?>>    requests = new LinkedList<LSMDBRequest<?>>();
    
    private final int                            maxQ;
    
    private boolean                              quit = true;
    private boolean                              graceful;
    
    public LSMDBWorker(BabuDBInternal babuDB, int id, int maxQ) {
        super("LSMDBWrkr#" + id);
        setLifeCycleListener(babuDB);
        this.maxQ = maxQ;
        this.dbs = babuDB;
    }
    
    public synchronized void addRequest(LSMDBRequest<?> request) throws InterruptedException {
        
        assert (request != null);
        
        // wait for queue space to become available
        if (!quit && maxQ > 0 && requests.size() >= maxQ) {
            wait();
        }
        
        if (!quit) {

            assert (maxQ == 0 || requests.size() < maxQ);
            
            requests.add(request);
            notifyAll();
        } else {
            throw new InterruptedException("Appending a request to the queue of " + getName() +
                        " was interrupted, due shutdown.");
        }
    }
    
    public synchronized void shutdown(boolean graceful) {
        this.graceful = graceful;
        
        quit = true;
        notifyAll();
    }
        
    @Override
    public void shutdown() {
        shutdown(true);
    }
    
    @Override
    public synchronized void start() {
        
        assert (quit);
        
        quit = false;
        super.start();
    }
    
    @Override
    public void run() {
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "operational");

        notifyStarted();
        
        while (!quit) {
            try {
                final LSMDBRequest<?> r;
                
                // wait for a request
                synchronized (this) {
                    if (requests.isEmpty()) {
                        wait();
                    }
                    
                    if (quit) {
                        break;
                        
                    // get a request
                    } else {
                        r = requests.poll();
                    }
                }
                                
                processRequest(r);
            } catch (InterruptedException ex) {
                if (!quit) {
                    cleanUp();
                    notifyCrashed(ex);
                    return;
                }
            }
        }
        
        // process pending requests on shutdown if graceful flag has not been reset
        if (graceful) {
            synchronized (requests) {
                for (LSMDBRequest<?> rq : requests) {
                    processRequest(rq);
                }
            }
        }
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "worker shutdown complete");
        notifyStopped();
    }
    
    /**
     * Closes the worker. And frees remaining requests.
     * 
     * @throws IOException
     */
    private synchronized void cleanUp() {    
            
        assert (graceful || requests.size() == 0);
        
        // clear pending requests, if available
        for (LSMDBRequest<?> rq : requests) {
            rq.getListener().failed(new BabuDBException(ErrorCode.INTERRUPTED, 
                "Worker was shut down, before the request could be proceeded."));
        }
    }
    
    @SuppressWarnings("unchecked")
    private void processRequest(LSMDBRequest<?> r) {
        switch (r.getOperation()) {
        case INSERT:
            doInsert(r);
            break;
        case LOOKUP:
            doLookup((LSMDBRequest<byte[]>) r);
            break;
        case PREFIX_LOOKUP:
            doPrefixLookup((LSMDBRequest<Iterator<Entry<byte[], byte[]>>>) r);
            break;
        case RANGE_LOOKUP:
            doRangeLookup((LSMDBRequest<Iterator<Entry<byte[], byte[]>>>) r);
            break;
        case USER_DEFINED_LOOKUP:
            doUserLookup((LSMDBRequest<Object>) r);
            break;
        case LOCK:
            doLock((LSMDBRequest<Object>) r);
            break;
        default:
            Logging.logMessage(Logging.LEVEL_ERROR, this,
                "UNKNOWN OPERATION REQUESTED! PROGRAMMATIC ERROR!!!! PANIC!");
            System.exit(1);
        }
    }
    
    private void doUserLookup(final LSMDBRequest<Object> r) {
        final UserDefinedLookup l = r.getUserDefinedLookup();
        final LSMLookupInterface lif = new LSMLookupInterface(r.getDatabase());
        try {
            Object result = l.execute(lif);
            r.getListener().finished(result);
        } catch (BabuDBException ex) {
            r.getListener().failed(ex);
        }
    }
    
    @SuppressWarnings("unchecked")
    private void doInsert(final LSMDBRequest<?> r) {

        try {
            dbs.getTransactionManager().makePersistent(
                    dbs.getDatabaseManager().createTransaction().insertRecordGroup(
                            r.getDatabase().getDatabaseName(), r.getInsertData(), r.getDatabase()), 
                            (BabuDBRequestResultImpl<Object>) r.getListener());
        } catch (BabuDBException e) {
            r.getListener().failed(e);
        }
    }
    
    private void doLookup(final LSMDBRequest<byte[]> r) {
        final LSMDatabase db = r.getDatabase();
        final int numIndices = db.getIndexCount();
        
        if ((r.getIndexId() >= numIndices) || (r.getIndexId() < 0)) {
            r.getListener().failed(
                new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + r.getIndexId() + 
                        " does not exist"));
        } else {
            r.getListener().finished(db.getIndex(r.getIndexId()).lookup(r.getLookupKey()));
        }
    }
    
    private void doPrefixLookup(final LSMDBRequest<Iterator<Map.Entry<byte[], byte[]>>> r) {
        final LSMDatabase db = r.getDatabase();
        final int numIndices = db.getIndexCount();
        
        if ((r.getIndexId() >= numIndices) || (r.getIndexId() < 0)) {
            r.getListener().failed(
                new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + r.getIndexId() + 
                        " does not exist"));
        } else {
            r.getListener().finished(db.getIndex(r.getIndexId()).prefixLookup(r.getLookupKey()));
        }
    }
    
    private void doRangeLookup(final LSMDBRequest<Iterator<Map.Entry<byte[], byte[]>>> r) {
        final LSMDatabase db = r.getDatabase();
        final int numIndices = db.getIndexCount();
        
        if ((r.getIndexId() >= numIndices) || (r.getIndexId() < 0)) {
            r.getListener().failed(
                new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + r.getIndexId() + 
                        " does not exist"));
        } else {
            r.getListener().finished(
                    db.getIndex(r.getIndexId()).rangeLookup(r.getFrom(), r.getTo()));
        }
    }
    
    private void doLock(final LSMDBRequest<Object> r) {

        synchronized (locked) {
            r.getListener().finished(locked);
            try {
                if (locked.get()) {
                    locked.wait();
                }
            } catch (InterruptedException e) {
                r.getListener().failed(
                        new BabuDBException(ErrorCode.INTERRUPTED, e.getMessage(), e));
            }
        }
    }
}