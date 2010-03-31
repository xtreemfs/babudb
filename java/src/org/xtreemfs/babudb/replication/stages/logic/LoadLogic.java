/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.clients.MasterClient;
import org.xtreemfs.babudb.interfaces.Chunk;
import org.xtreemfs.babudb.interfaces.DBFileMetaData;
import org.xtreemfs.babudb.interfaces.DBFileMetaDataSet;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.errnoException;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.operations.ErrNo;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.babudb.replication.stages.ReplicationStage.ConnectionLostException;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.foundation.oncrpc.client.RPCResponseAvailableListener;
import org.xtreemfs.interfaces.utils.ONCRPCException;

import static org.xtreemfs.babudb.replication.DirectFileIO.*;
import static org.xtreemfs.babudb.replication.stages.logic.LogicID.*;

/**
 * <p>Performs an initial load request at the master.
 * This is an all-or-nothing mechanism.</p>
 * @author flangner
 * @since 06/08/2009
 */

public class LoadLogic extends Logic {
    
    public LoadLogic(ReplicationStage stage) {
        super(stage);
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
                new LSN(stage.missing.getViewId(),stage.missing.getSequenceEnd());
        
        if (stage.missing != null && 
            stage.missing.getSequenceEnd() == stage.missing.getSequenceStart()) {
            stage.missing = null;
        }
                
        MasterClient master = SharedLogic.getSynchronizationPartner(
                stage.dispatcher.getConfig().getParticipants(), until, 
                stage.dispatcher.master);
        
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
            DiskLogger logger = stage.dispatcher.dbs.getLogger();
            try {
                logger.lockLogger();
                logger.switchLogFile(true);
            } catch (IOException e) {
                // system failure on switching the lock file --> retry
                Logging.logError(Logging.LEVEL_WARN, this, e);
                return;
            } finally {
                logger.unlockLogger();
            }
            Logging.logMessage(Logging.LEVEL_DEBUG, this, 
            "Logfile switched at LSN %s.", stage.lastInserted.toString());
            
            stage.lastInserted = new LSN(stage.lastInserted.getViewId()+1,0L);
            loadFinished(false);
            return;
        }
        
        // backup the old dbs and stop the heartBeat
        stage.dispatcher.heartbeat.infarction();
        stage.dispatcher.dbs.stop();
        try {
            backupFiles(stage.dispatcher.getConfig());
        } catch (IOException e) {
            // file backup failed --> retry
            Logging.logError(Logging.LEVEL_WARN, this, e);
            
            if (stage.isInterrupted()) {
                try {
                    stage.dispatcher.dbs.restart();
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
            // if we got an empty file
            if (!(fileSize > 0L)) return;
            assert (maxChunkSize > 0L) : "Empty chunks are not allowed: "+fileName;
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
                                    Logging.logMessage(Logging.LEVEL_ERROR, this, "CHUNK ERROR: %s", 
                                            "Empty buffer received!");
                                    stage.interrupt();
                                }
                                // insert the file input
                                File f = getFile(chunk);
                                assert (f.exists()) : "File was not created properly: "+chunk.toString();
                                fChannel = new FileOutputStream(f).getChannel();
                                fChannel.write(buffer.getBuffer());
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
            stage.dispatcher.updateLatestLSN(
                    stage.lastInserted = stage.dispatcher.dbs.restart());
            removeBackupFiles(stage.dispatcher.getConfig());
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
        if (stage.missing != null && stage.missing.getSequenceEnd() > 
            stage.lastInserted.getSequenceNo()+1L) {
            stage.missing = new LSNRange(stage.lastInserted.getViewId(),
            stage.lastInserted.getSequenceNo()+1L,
            stage.missing.getSequenceEnd());
            stage.setLogic(REQUEST, "There are still some logEntries " +
                    "missing after switching the logfile.");
        } else {
            String msg = (loaded) ? "Loading finished with LSN("+stage.
                    lastInserted+"), we can go on with the basicLogic." : 
                    "Only the viewId changed, we can go on with the basicLogic.";
            stage.missing = null;
            stage.setLogic(BASIC, msg, loaded);
        }
    }
    
    /**
     * Prepares the file to the given chunk.
     * 
     * @param chunk
     * @return the {@link File} to the given {@link Chunk}.
     * @throws IOException
     */
    private File getFile(Chunk chunk) throws IOException {
        File chnk = new File(chunk.getFileName());
        String fName = chnk.getName();
        String pName = chnk.getParentFile().getName();
        File result;
        String baseDir = stage.dispatcher.dbs.getConfig().getBaseDir();
        
        if (LSMDatabase.isSnapshotFilename(pName)) {
            // create the db-name directory, if necessary
            new File(baseDir + chnk.getParentFile().getName() + File.separatorChar)
                    .mkdirs();
            // create the file if necessary
            result = new File(baseDir + 
                         chnk.getParentFile().getParentFile().getName() +
                         File.separatorChar +  pName +
                         File.separator + fName);
            result.getParentFile().mkdirs();
            result.createNewFile();
        } else if (chnk.getParent() == null) {
            // create the file if necessary
            result = new File(baseDir +
                    stage.dispatcher.dbs.getConfig().getDbCfgFile());
            result.getParentFile().mkdirs();
            result.createNewFile();
        } else {
            // create the file if necessary
            result = new File(stage.dispatcher.dbs.getConfig().getDbLogDir()
                    + fName);
            result.getParentFile().mkdirs();
            result.createNewFile();
        }
        return result;
    }
}