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

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSMDatabase.DBFileMetaData;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.Pacemaker;
import org.xtreemfs.babudb.replication.service.ReplicationStage;
import org.xtreemfs.babudb.replication.service.ReplicationStage.Range;
import org.xtreemfs.babudb.replication.service.SlaveView;
import org.xtreemfs.babudb.replication.service.ReplicationStage.ConnectionLostException;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.ClientResponseAvailableListener;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.transmission.ErrorCode;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.PBRPCClientAdapter.ErrorCodeException;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

import static org.xtreemfs.babudb.replication.service.logic.LogicID.*;

/**
 * <p>Performs an initial load request at the master.
 * This is an all-or-nothing mechanism.</p>
 * @author flangner
 * @since 06/08/2009
 */

public class LoadLogic extends Logic {
    
    private final int maxChunkSize;
    
    /**
     * @param stage
     * @param pacemaker
     * @param slaveView
     * @param fileIO
     * @param babuInterface
     */
    public LoadLogic(ReplicationStage stage, Pacemaker pacemaker, 
            SlaveView slaveView, FileIOInterface fileIO, int maxChunkSize) {
        super(stage, pacemaker, slaveView, fileIO);
        
        this.maxChunkSize = maxChunkSize;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.stages.logic.Logic#getId()
     */
    @Override
    public LogicID getId() {
        return LOAD;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() throws ConnectionLostException, InterruptedException{
        LSN actual = stage.getBabuDB().getState();
        LSN until = (stage.missing == null) ? new LSN(actual.getViewId() + 1,0L) : 
                                              stage.missing.end;
                
        MasterClient master = slaveView.getSynchronizationPartner(until);
        
        // make the request and get the result synchronously
        Logging.logMessage(Logging.LEVEL_INFO, stage, "Loading DB since %s from %s.", 
                actual.toString(), master.getDefaultServerAddress().toString());
        ClientResponseFuture<DBFileMetaDataSet> rp = master.load(actual);
        DBFileMetaDataSet result = null;
        try {
            result = rp.get();  
        } catch (ErrorCodeException e) {
            // connection is lost
            throw new ConnectionLostException(e.getMessage(), e.getCode());
        } catch (IOException e) {
            // failure on transmission --> retry
            throw new ConnectionLostException(e.getMessage(), ErrorCode.UNKNOWN);
        }
        
        // switch log file by triggering a manual checkpoint, 
        // if the response was empty
        if (result.size() == 0) {
            
            try {
                stage.lastOnView.set(stage.getBabuDB().checkpoint());
            } catch (BabuDBException e) {
                // system failure on switching the lock file --> retry
                Logging.logError(Logging.LEVEL_WARN, this, e);
                return;
            } 
            Logging.logMessage(Logging.LEVEL_DEBUG, this, 
            "Logfile switched at LSN %s.", stage.getBabuDB().getState().toString());
            
            finished(false);
            return;
        }
        
        // backup the old dbs
        stage.getBabuDB().stopBabuDB();
        try {
            fileIO.backupFiles();
        } catch (IOException e) {
            // file backup failed --> retry
            Logging.logError(Logging.LEVEL_WARN, this, e);
            
            if (stage.isInterrupted()) {
                try {
                    stage.getBabuDB().startBabuDB();
                } catch (BabuDBException e1) {
                    Logging.logError(Logging.LEVEL_ERROR, this, e1);
                }
            }
            return;
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
                if (lsn == null) 
                    lsn = LSMDatabase.getSnapshotLSNbyFilename(parentName);
                else if (!lsn.equals(LSMDatabase.
                        getSnapshotLSNbyFilename(parentName))){
                    Logging.logMessage(Logging.LEVEL_WARN, this, 
                            "Indexfiles had ambiguous LSNs: %s", 
                            "LOAD will be retried.");
                    return;
                }
            }
            long fileSize = fileData.size;
            
            // if we got an empty file, that cannot be right, so try again
            if (!(fileSize > 0L)) return;
            
            assert (maxChunkSize > 0L) : 
                "Empty chunks are not allowed: " + fileName;
            
            synchronized (openChunks) {
                if (openChunks.get() != -1)
                    openChunks.addAndGet((int) (fileSize / maxChunkSize));
            }
                
            // calculate chunks, request them and add them to the list
            long begin = 0L;
            for (long end = maxChunkSize; end < (fileSize+maxChunkSize); end += maxChunkSize) {
                                
                // request the chunk
                final long pos1 = begin;
                final long size = (end > fileSize) ? fileSize : end;
                final ClientResponseFuture<ReusableBuffer> chunkRp = 
                    master.chunk(fileName, pos1, size);       
                begin = end;
                chunkRp.registerListener(new ClientResponseAvailableListener<ReusableBuffer>() {
                
                    @Override
                    public void responseAvailable(ReusableBuffer buffer) {
                        // insert the chunk
                        FileChannel fChannel = null;
                        try {
                            if (buffer.remaining() == 0){
                                Logging.logMessage(Logging.LEVEL_ERROR, this, 
                                    "CHUNK ERROR: Empty buffer received!");
                                
                                stage.interrupt();
                            }
                            // insert the file input
                            File f = fileIO.getFile(fileName);
                            Logging.logMessage(Logging.LEVEL_INFO, this, 
                                    "SAVING %s to %s.", fileName, f.getPath());
                            
                            assert (f.exists()) : "File '" + fileName + 
                                    "' was not created properly.";
                            fChannel = new FileOutputStream(f).getChannel();
                            fChannel.write(buffer.getBuffer(), pos1);
                        } catch (IOException e) {
                            Logging.logMessage(Logging.LEVEL_ERROR, this, 
                                    "Chunk request (%s,%d,%d) failed: %s", 
                                    fileName, pos1, e.getMessage());
                        } finally {
                            if (fChannel!=null) {
                                try {
                                    fChannel.close();
                                } catch (IOException e) {
                                    Logging.logError(Logging.LEVEL_ERROR, 
                                            this, e);
                                    
                                }
                            }
                            if (buffer!=null) BufferPool.free(buffer);
                        }
                        
                        // notify, if the last chunk was inserted
                        synchronized (openChunks) {
                            if (openChunks.get() != -1 && openChunks.
                                    decrementAndGet() == 0) 
                                openChunks.notify();
                        }
                    }

                    @Override
                    public void requestFailed(Exception e) {
                        if (e instanceof ErrorCodeException) {
                            ErrorCodeException err = (ErrorCodeException) e;
                            Logging.logMessage(Logging.LEVEL_ERROR, this,
                                   "Chunk request (%s,%d,%d) failed: (%d) %s", 
                                   fileName, pos1, err.getCode());
                        } else {
                            Logging.logMessage(Logging.LEVEL_ERROR, this, 
                                "Chunk request (%s,%d,%d) failed: %s", 
                                fileName, pos1, e.getMessage());
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
            if (openChunks.get()>0)
                openChunks.wait();
        }

        // some chunks failed -> retry
        if (openChunks.get() == -1) return;
        
        // reload the DBS
        try {
            stage.getBabuDB().startBabuDB();
            fileIO.removeBackupFiles();
            finished(true);
        } catch (BabuDBException e) {
            // resetting the DBS failed --> retry
            Logging.logMessage(Logging.LEVEL_WARN, this, "Loading failed, because the " +
            		"reloading the DBS failed due: %s", e.getMessage());
        }
    }
    
    /**
     * Method to execute, after the Load was finished successfully.
     * 
     * @param loaded - induces whether the DBS had to be reload or not
     */
    private void finished(boolean loaded) {
        LSN actual = stage.getBabuDB().getState();
        LSN next = new LSN (actual.getViewId(), actual.getSequenceNo() + 1L);

        if (stage.missing != null && 
            next.compareTo(stage.missing.end) < 0) {
            
            stage.missing = new Range(actual, stage.missing.end);
            
            stage.setLogic(REQUEST, "There are still some logEntries " +
                    "missing after loading the database.");
        } else {
            String msg = (loaded) ? "Loading finished with LSN(" + actual + 
                    "), we can go on with the basicLogic." : 
                    "Only the viewId changed, we can go on with the basicLogic.";
            stage.missing = null;
            stage.setLogic(BASIC, msg, loaded);
        }
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