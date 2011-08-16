/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.logic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSMDatabase.DBFileMetaData;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.service.SlaveView;
import org.xtreemfs.babudb.replication.service.StageRequest;
import org.xtreemfs.babudb.replication.service.ReplicationStage.StageCondition;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.ClientResponseAvailableListener;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter.ErrorCodeException;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * <p>Performs an initial load request at the master. This is an all-or-nothing mechanism.</p>
 * @author flangner
 * @since 06/08/2009
 */

public class LoadLogic extends Logic {
    
    private final int maxChunkSize;
    
    /**
     * @param babuDB
     * @param slaveView
     * @param fileIO
     * @param maxChunkSize
     * @param lastOnView
     */
    public LoadLogic(BabuDBInterface babuDB, SlaveView slaveView, FileIOInterface fileIO, int maxChunkSize, 
            AtomicReference<LSN> lastOnView) {
        
        super(babuDB, slaveView, fileIO, lastOnView);
        
        this.maxChunkSize = maxChunkSize;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.stages.logic.Logic#getId()
     */
    @Override
    public LogicID getId() {
        return LogicID.LOAD;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.logic.Logic#
     *      run(org.xtreemfs.babudb.replication.service.ReplicationStage.StageCondition)
     */
    @Override
    public StageCondition run(StageCondition condition) throws InterruptedException {
        
        assert (condition.logicID == LogicID.LOAD)  : "PROGRAMATICAL ERROR!";
        
        LSN actual = getState();
        LSN until = (condition.end == null) ? new LSN(actual.getViewId() + 1,0L) : condition.end;
                
        MasterClient master = slaveView.getSynchronizationPartner(until);
        
        // make the request and get the result synchronously
        Logging.logMessage(Logging.LEVEL_INFO, this, "LOAD: Loading DB since %s from %s.", 
                actual.toString(), master.getDefaultServerAddress().toString());
        
        DBFileMetaDataSet result = null;
        try {
            result = master.load(actual).get();  
        } catch (Exception e) {
            Logging.logMessage(Logging.LEVEL_INFO, this, "LOAD: metadata could not be retrieved from Master (%s).", 
                    master.toString()); // XXX
            Logging.logError(Logging.LEVEL_INFO, this, e); // XXX
            
            // failure on transmission -> retry
            return condition;
        }
        
        // switch log file by triggering a manual checkpoint, 
        // if the response was empty
        if (result.size() == 0) {
            
            LSN lov;
            try {
                lov = babuDB.checkpoint();
                lastOnView.set(lov);
                
                Logging.logMessage(Logging.LEVEL_INFO, this, "LOAD: Logfile switched at LSN %s.", lov.toString()); // XXX
                
                return finish(until);
            } catch (BabuDBException e) {
                Logging.logMessage(Logging.LEVEL_WARN, this, "LOAD: Taking a checkpoint failed.");
                Logging.logError(Logging.LEVEL_WARN, this, e);
                
                // system failure on switching the lock file -> retry
                return condition;
            } 
        }
        
        // backup the old dbs
        babuDB.stopBabuDB();
        try {
            fileIO.backupFiles();
        } catch (IOException e) {
            // file backup failed -> retry
            Logging.logMessage(Logging.LEVEL_WARN, this, "LOAD: File-backup failed.");
            Logging.logError(Logging.LEVEL_WARN, this, e);
            
            if (Thread.interrupted()) {
                try {
                    babuDB.startBabuDB();
                } catch (BabuDBException e1) {
                    Logging.logError(Logging.LEVEL_ERROR, this, e1);
                }
            }
            return condition;
        }
        
        // #chunks >= #files
        final AtomicInteger openChunks = new AtomicInteger(result.size()); 
        
        // request the chunks
        LSN lsn = null;
        for (DBFileMetaData fileData : result) {
            
            // validate the informations
            final String fileName = fileData.file;
            String parentName = new File(fileName).getParentFile().getName();
            if (LSMDatabase.isSnapshotFilename(parentName)) {
                if (lsn == null) {
                    lsn = LSMDatabase.getSnapshotLSNbyFilename(parentName);
                } else if (!lsn.equals(LSMDatabase.getSnapshotLSNbyFilename(parentName))) {
                    Logging.logMessage(Logging.LEVEL_INFO, this, "LOAD: Indexfiles had ambiguous LSNs: %s", 
                            "LOAD will be retried."); //XXX
                    return condition;
                }
            }
            long fileSize = fileData.size;
            
            // if we got an empty file, that cannot be right, so try again
            if (!(fileSize > 0L)) {
                Logging.logMessage(Logging.LEVEL_INFO, this, "LOAD: Empty file received -> retry."); // XXX
                return condition;
            }
            
            assert (maxChunkSize > 0L) : 
                "Empty chunks are not allowed: " + fileName;
            
            synchronized (openChunks) {
                if (openChunks.get() != -1) {
                    openChunks.addAndGet((int) (fileSize / maxChunkSize));
                }
            }
                
            // calculate chunks, request them and add them to the list
            long begin = 0L;
            for (long end = maxChunkSize; end < (fileSize+maxChunkSize); end += maxChunkSize) {
                                
                // request the chunk
                final long pos1 = begin;
                final long size = (end > fileSize) ? fileSize : end;
                begin = end;
                master.chunk(fileName, pos1, size).registerListener(
                        new ClientResponseAvailableListener<ReusableBuffer>() {
                
                    @Override
                    public void responseAvailable(ReusableBuffer buffer) {
                                       
                        synchronized (openChunks) {
                                
                            // insert the chunk
                            FileChannel fChannel = null;
                            try {
                                if (openChunks.get() < 0) throw new IOException();
                                
                                if (buffer.remaining() == 0){
                                    Logging.logMessage(Logging.LEVEL_ERROR, this, 
                                        "LOAD: CHUNK ERROR: Empty buffer received!"); // XXX
                                    
                                    throw new IOException("CHUNK ERROR: Empty buffer received!");
                                }
                                // insert the file input
                                File f = fileIO.getFile(fileName);
                                Logging.logMessage(Logging.LEVEL_INFO, this, 
                                        "LOAD: SAVING %s to %s.", fileName, f.getPath()); //XXX
                                
                                assert (f.exists()) : "File '" + fileName + 
                                        "' was not created properly.";
                                fChannel = new FileOutputStream(f).getChannel();
                                fChannel.write(buffer.getBuffer(), pos1);
                            } catch (IOException e) {
                                
                                Logging.logMessage(Logging.LEVEL_ERROR, this, 
                                        "LOAD: Chunk request (%s,%d,%d) failed: %s", 
                                        fileName, pos1, size, e.getMessage()); //XXX
                                
                                openChunks.set(-1);
                                openChunks.notify();
                                return;
                            } finally {
                                if (fChannel!=null) {
                                    try {
                                        fChannel.close();
                                    } catch (IOException e) {
                                        Logging.logError(Logging.LEVEL_ERROR, this, e);
                                        
                                    }
                                }
                                if (buffer!=null) BufferPool.free(buffer);
                            }
                        
                            // notify, if the last chunk was inserted
                            if (openChunks.get() != -1 && openChunks.decrementAndGet() == 0) openChunks.notify();
                        }
                    }

                    @Override
                    public void requestFailed(Exception e) {
                        if (e instanceof ErrorCodeException) {
                            ErrorCodeException err = (ErrorCodeException) e;
                            Logging.logMessage(Logging.LEVEL_ERROR, this,
                                   "LOAD: Chunk request (%s,%d,%d) failed: (%d) %s", 
                                   fileName, pos1, size, err.getCode()); // XXX
                        } else {
                            Logging.logMessage(Logging.LEVEL_ERROR, this, 
                                "LOAD: Chunk request (%s,%d,%d) failed: %s", 
                                fileName, pos1, size, e.getMessage()); //XXX
                        }
                        
                        synchronized (openChunks) {
                            openChunks.set(-1);
                            openChunks.notify();
                        }
                    }
                });
            }        
        }
    
        // wait for the last response
        synchronized (openChunks) {
            if (openChunks.get() > 0) openChunks.wait();
        }

        // some chunks failed -> retry
        if (openChunks.get() == -1) { 
            Logging.logMessage(Logging.LEVEL_INFO, this, "LOAD: At least one chunk could not have been inserted."); // XXX
            return condition;
        }
        
        // reload the DBS
        try {
            babuDB.startBabuDB();
            fileIO.removeBackupFiles();
            return finish(until);
        } catch (BabuDBException e) {
            // resetting the DBS failed -> retry
            Logging.logMessage(Logging.LEVEL_WARN, this, "LOAD: Loading failed, because the " +
                    "reloading the DBS failed due: %s", e.getMessage());
            return condition;
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.logic.Logic#
     *      run(org.xtreemfs.babudb.replication.service.StageRequest)
     */
    @Override
    public StageCondition run(StageRequest rq) {
        throw new UnsupportedOperationException("PROGRAMATICAL ERROR!");
    }
    
    public final static class DBFileMetaDataSet extends ArrayList<DBFileMetaData> {
        private static final long serialVersionUID = -6430317150111369328L;
        
        final int chunkSize;
        
        public DBFileMetaDataSet(int chunkSize, Collection<DBFileMetaData> c) {
            super(c);
            this.chunkSize = chunkSize;
        }
    }
}