/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
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
import org.xtreemfs.babudb.interfaces.Chunk;
import org.xtreemfs.babudb.interfaces.DBFileMetaData;
import org.xtreemfs.babudb.interfaces.DBFileMetaDataSet;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCError;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCException;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.operations.ErrNo;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.babudb.replication.stages.ReplicationStage.ConnectionLostException;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponseAvailableListener;

import static org.xtreemfs.babudb.replication.DirectFileIO.*;
import static org.xtreemfs.babudb.replication.stages.logic.LogicID.*;

/**
 * <p>Performs an initial load request at the master.
 * This is an all-or-nothing operation.</p>
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
        // make the request and get the result synchronously
        Logging.logMessage(Logging.LEVEL_INFO, stage, "Loading from: %s", stage.lastInserted.toString());
        RPCResponse<DBFileMetaDataSet> rp = stage.dispatcher.master.load(stage.lastInserted);
        DBFileMetaDataSet result = null;
        try {
            result = rp.get();  
        } catch (ONCRPCException e) {
            // connection is lost
            int errNo = (e != null && e instanceof ONCRPCError) ? 
                    ((ONCRPCError) e).getAcceptStat() : ErrNo.UNKNOWN;
            throw new ConnectionLostException(e.getTypeName()+": "+e.getMessage(),errNo);
        } catch (IOException e) {
            // failure on transmission --> retry
            throw new ConnectionLostException(e.getMessage(),ErrNo.UNKNOWN);
        } finally {
            if (rp!=null) rp.freeBuffers();
        }
        
        // switch log file, if the response was empty
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
            Logging.logMessage(Logging.LEVEL_INFO, this, "Logfile switched with at LSN: %s", stage.lastInserted.toString());
            stage.lastInserted = new LSN(stage.lastInserted.getViewId()+1,0L);
            stage.setLogic(BASIC, "Only the viewId changed, we can go on with the basicLogic.");
            return;
        }
        
        // backup the old dbs and stop the heartBeat
        stage.dispatcher.heartbeat.infarction();
        
        try {
            backupFiles(stage.dispatcher.configuration);
        } catch (IOException e) {
            // file backup failed --> retry
            Logging.logError(Logging.LEVEL_WARN, this, e);
            return;
        }
        
        // #chunks >= #files
        final AtomicInteger openChunks = new AtomicInteger(result.size()); 
        
        try {
            synchronized (openChunks) {
                // request the chunks
                LSN lsn = null;
                for (DBFileMetaData fileData : result) {
                    System.err.println(result.toString());
                    
                    // validate the informations
                    String fileName = fileData.getFileName();
                    if (LSMDatabase.isSnapshotFilename(fileName)) {
                        if (lsn == null) lsn = LSMDatabase.getSnapshotLSNbyFilename(fileName);
                        else if (!lsn.equals(LSMDatabase.getSnapshotLSNbyFilename(fileName))){
                            Logging.logMessage(Logging.LEVEL_WARN, this, "Indexfiles had ambiguous LSNs: %s", "LOAD will be retried.");
                            return;
                        }
                    }
                    long fileSize = fileData.getFileSize();
                    long maxChunkSize = fileData.getMaxChunkSize();
                    // if we got an empty file
                    if (!(fileSize > 0L)) return;
                    assert (maxChunkSize > 0L) : "Empty chunks are not allowed: "+fileName;
                    openChunks.addAndGet((int) (fileSize / maxChunkSize));
                        
                    // calculate chunks, request them and add them to the list
                    long begin = 0L;
                    for (long end = maxChunkSize; end < (fileSize+maxChunkSize); end += maxChunkSize) {
                        final Chunk chunk = new Chunk(fileName, begin, (end>fileSize) ? fileSize : end);
                        begin = end;
                        
                        // request the chunk
                        final RPCResponse<ReusableBuffer> chunkRp = stage.dispatcher.master.chunk(chunk);       
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
                                    if (openChunks.decrementAndGet() == 0) {
                                        synchronized (openChunks) {
                                            openChunks.notify();
                                        }
                                    }
                                } catch (Exception e) {
                                    Logging.logMessage(Logging.LEVEL_ERROR, this, "Chunk request failed: %s", e.getMessage());
                                    
                                    // make a new initial load
                                    stage.interrupt();
                                } finally {
                                    if (r!=null) r.freeBuffers();
                                }
                            }
                        });
                    }        
                }
    
                // wait for the last response
                openChunks.wait();
            }
        } catch (InterruptedException e) {
            // if interrupted by a failed chunk-write --> retry
            if (!stage.isTerminating()) return;
            // else drop the exception to the replication stage
            else throw e;
        }
        
        // reload the DBS
        try {
            stage.dispatcher.updateLatestLSN(
                    stage.lastInserted = stage.dispatcher.dbs.reset());
            removeBackupFiles(stage.dispatcher.configuration);
            stage.setLogic(BASIC, "Loading finished, we can go on with the basicLogic.");
        } catch (BabuDBException e) {
            // resetting the DBS failed --> retry
            Logging.logMessage(Logging.LEVEL_WARN, this, "Loading failed, because the " +
            		"reloading the DBS failed due: %s", e.getMessage());
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
        File result;
        String baseDir = stage.dispatcher.dbs.getConfig().getBaseDir();
        
        if (LSMDatabase.isSnapshotFilename(fName)) {
            // create the db-name directory, if necessary
            new File(baseDir + chnk.getParentFile().getName() + File.separatorChar)
                    .mkdirs();
            // create the file if necessary
            result = new File(baseDir + chnk.getParentFile().getName() 
                    + File.separatorChar + fName);
            result.createNewFile();
        } else if (chnk.getParent() == null) {
            // create the file if necessary
            result = new File(baseDir +
                    stage.dispatcher.dbs.getConfig().getDbCfgFile());
            result.createNewFile();
        } else {
            // create the file if necessary
            result = new File(stage.dispatcher.dbs.getConfig().getDbLogDir()
                    + fName);
            result.createNewFile();
        }
        return result;
    }
}