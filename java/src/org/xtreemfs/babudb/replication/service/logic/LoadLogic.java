/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.interfaces.Chunk;
import org.xtreemfs.babudb.interfaces.DBFileMetaData;
import org.xtreemfs.babudb.interfaces.DBFileMetaDataSet;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.errnoException;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.service.Pacemaker;
import org.xtreemfs.babudb.replication.service.ReplicationStage;
import org.xtreemfs.babudb.replication.service.SlaveView;
import org.xtreemfs.babudb.replication.service.ReplicationStage.ConnectionLostException;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.ErrNo;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.foundation.oncrpc.client.RPCResponseAvailableListener;
import org.xtreemfs.foundation.oncrpc.utils.ONCRPCException;

import static org.xtreemfs.babudb.replication.service.logic.LogicID.*;

/**
 * <p>Performs an initial load request at the master.
 * This is an all-or-nothing mechanism.</p>
 * @author flangner
 * @since 06/08/2009
 */

public class LoadLogic extends Logic {

    private final AtomicReference<LSN> lastOnView;
    
    /**
     * @param lastOnView
     * @param stage
     * @param pacemaker
     * @param slaveView
     * @param fileIO
     * @param babuInterface
     */
    public LoadLogic(ReplicationStage stage, Pacemaker pacemaker, 
            SlaveView slaveView, FileIOInterface fileIO, 
            BabuDBInterface babuInterface, AtomicReference<LSN> lastOnView) {
        super(stage, pacemaker, slaveView, fileIO, babuInterface);
        
        this.lastOnView = lastOnView;
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
        LSN until = (stage.missing == null) ? 
                new LSN(stage.lastInserted.getViewId()+1,0L) :
                new LSN(stage.missing.getEnd());
        
        if (stage.missing != null && 
            new LSN(stage.missing.getStart())
                .equals(new LSN(stage.missing.getEnd()))) {
            stage.missing = null;
        }
                
        MasterClient master = this.slaveView.getSynchronizationPartner(until);
        
        // make the request and get the result synchronously
        Logging.logMessage(Logging.LEVEL_INFO, stage, "Loading DB since %s from %s.", 
                stage.lastInserted.toString(), master.getDefaultServerAddress().toString());
        RPCResponse<DBFileMetaDataSet> rp = master.load(stage.lastInserted);
        DBFileMetaDataSet result = null;
        try {
            result = rp.get();  
        } catch (ONCRPCException e) {
            // connection is lost
            int errNo = (e != null && e instanceof errnoException) ? 
                    ((errnoException) e).getError_code() : ErrNo.UNKNOWN;
            throw new ConnectionLostException(e.getTypeName()+": "+e.getMessage(),errNo);
        } catch (IOException e) {
            // failure on transmission --> retry
            throw new ConnectionLostException(e.getMessage(),ErrNo.UNKNOWN);
        } finally {
            if (rp!=null) rp.freeBuffers();
        }
        
        // switch log file by triggering a manual checkpoint, 
        // if the response was empty
        if (result.size() == 0) {
            
            try {
                this.lastOnView.set(this.babuInterface.switchLogFile());
                stage.lastInserted = this.babuInterface.getState();
            } catch (IOException e) {
                // system failure on switching the lock file --> retry
                Logging.logError(Logging.LEVEL_WARN, this, e);
                return;
            } 
            Logging.logMessage(Logging.LEVEL_DEBUG, this, 
            "Logfile switched at LSN %s.", stage.lastInserted.toString());
            
            loadFinished(false);
            return;
        }
        
        // backup the old dbs
        this.babuInterface.stopBabuDB();
        try {
            this.fileIO.backupFiles();
        } catch (IOException e) {
            // file backup failed --> retry
            Logging.logError(Logging.LEVEL_WARN, this, e);
            
            if (stage.isInterrupted()) {
                try {
                    this.babuInterface.startBabuDB();
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
            String fileName = fileData.getFileName();
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
            long fileSize = fileData.getFileSize();
            long maxChunkSize = fileData.getMaxChunkSize();
            
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
                final Chunk chunk = new Chunk(fileName, begin, (end>fileSize) ? fileSize : end);
                begin = end;
                
                // request the chunk
                final RPCResponse<ReusableBuffer> chunkRp = master.chunk(chunk);       
                chunkRp.registerListener(new RPCResponseAvailableListener<ReusableBuffer>() {
                
                    @Override
                    public void responseAvailable(RPCResponse<ReusableBuffer> r) {
                        try {
                            // insert the chunk
                            ReusableBuffer buffer = r.get();
                            
                            FileChannel fChannel = null;
                            try {
                                if (buffer.remaining() == 0){
                                    Logging.logMessage(Logging.LEVEL_ERROR, this, 
                                        "CHUNK ERROR: Empty buffer received!");
                                    
                                    stage.interrupt();
                                }
                                // insert the file input
                                File f = fileIO.getFile(chunk);
                                assert (f.exists()) : 
                                    "File was not created properly: " + 
                                    chunk.toString();
                                fChannel = new FileOutputStream(f).getChannel();
                                fChannel.write(buffer.getBuffer(), 
                                        chunk.getBegin());
                            } finally {
                                if (fChannel!=null) fChannel.close();
                                if (buffer!=null) BufferPool.free(buffer);
                            }
                            
                            // notify, if the last chunk was inserted
                            synchronized (openChunks) {
                                if (openChunks.get() != -1 && openChunks.
                                        decrementAndGet() == 0) 
                                    openChunks.notify();
                            }
                        } catch (Exception e) {
                            if (e instanceof errnoException) {
                                errnoException err = (errnoException) e;
                                Logging.logMessage(Logging.LEVEL_ERROR, this,
                                       "Chunk request (%s) failed: (%d) %s", 
                                       chunk.toString(), err.getError_code(), 
                                       err.getError_message());
                            } else
                                Logging.logMessage(Logging.LEVEL_ERROR, this, 
                                    "Chunk request (%s) failed: %s", 
                                    chunk.toString(), e.getMessage());
                            
                            synchronized (openChunks) {
                                openChunks.set(-1);
                                openChunks.notify();
                            }
                        } finally {
                            if (r!=null) r.freeBuffers();
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
            stage.lastInserted = this.babuInterface.startBabuDB();
            this.fileIO.removeBackupFiles();
            loadFinished(true);
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
    private void loadFinished(boolean loaded) {
        if (stage.missing != null && 
            stage.lastInserted.compareTo(new LSN(stage.missing.getEnd())) < 0) {
            
            stage.missing = new LSNRange(
                    new org.xtreemfs.babudb.interfaces.LSN(
                            stage.lastInserted.getViewId(),
                            stage.lastInserted.getSequenceNo()),
                    stage.missing.getEnd());
            
            stage.setLogic(REQUEST, "There are still some logEntries " +
                    "missing after switching the log-file.");
        } else {
            String msg = (loaded) ? "Loading finished with LSN("+stage.
                    lastInserted+"), we can go on with the basicLogic." : 
                    "Only the viewId changed, we can go on with the basicLogic.";
            stage.missing = null;
            stage.setLogic(BASIC, msg, loaded);
        }
    }
}