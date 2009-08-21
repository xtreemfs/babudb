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
import java.util.ArrayList;
import java.util.List;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.interfaces.Chunk;
import org.xtreemfs.babudb.interfaces.DBFileMetaData;
import org.xtreemfs.babudb.interfaces.DBFileMetaDataSet;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCException;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
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
        final List<Chunk> chunks = new ArrayList<Chunk>();
        
        // make the request and get the result synchronously
        Logging.logMessage(Logging.LEVEL_INFO, stage, "Loading from: %s", stage.loadUntil.toString());
        RPCResponse<DBFileMetaDataSet> rp = stage.dispatcher.master.load(stage.loadUntil);
        DBFileMetaDataSet result = null;
        try {
            result = rp.get();  
        } catch (ONCRPCException e) {
            // connection is lost
            throw new ConnectionLostException(e.getMessage());
        } catch (IOException e) {
            // failure on transmission --> retry
            throw new ConnectionLostException(e.getMessage());
        } finally {
            if (rp!=null) rp.freeBuffers();
        }
        
        // switch log file
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
            Logging.logMessage(Logging.LEVEL_INFO, this, "Logfile switched with at LSN: %s", stage.loadUntil.toString());
            stage.loadUntil = new LSN(stage.loadUntil.getViewId()+1,0L);
            stage.setLogic(BASIC);
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
        
        for (DBFileMetaData fileData : result) {
            
            // validate the informations
            String fileName = fileData.getFileName();
            long fileSize = fileData.getFileSize();
            long maxChunkSize = fileData.getMaxChunkSize();
            assert (fileSize > 0L) : "Empty files are not allowed.";
            assert (maxChunkSize > 0L) : "Empty chunks are not allowed.";
            
            // calculate chunks, request them and add them to the list
            long begin = 0L;
            for (long end = maxChunkSize; end < fileSize; end += maxChunkSize) {
                final Chunk chunk = new Chunk(fileName, begin, end);
                chunks.add(chunk);
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
                                // insert the file input
                                fChannel = new FileOutputStream(getFile(chunk)).getChannel();
                                fChannel.write(buffer.getBuffer());
                            } finally {
                                if (fChannel!=null) fChannel.close();
                                if (buffer!=null) BufferPool.free(buffer);
                            }
                            
                            // notify, if the last chunk was inserted
                            synchronized (chunks) {
                                chunks.remove(chunk);
                                if (chunks.isEmpty())
                                    chunks.notify();
                            }
                        } catch (Exception e) {
                            // make a new initial load
                            stage.interrupt();
                        } finally {
                            if (r!=null) r.freeBuffers();
                        }
                        chunkRp.freeBuffers();
                    }
                });
            }        
        }
    
        // wait for the last response
        try {
            synchronized (chunks) {
                if (!chunks.isEmpty())
                    chunks.wait();
            }
        } catch (InterruptedException e) {
            // if interrupted by a failed chunk-write --> retry
            if (!stage.isTerminating()) return;
            // else drop the exception to the stage
            else throw e;
        }

        // reload the DBS
        try {
            stage.dispatcher.updateLatestLSN(stage.dispatcher.dbs.reset());
        } catch (BabuDBException e) {
            // resetting the DBS failed --> retry
            Logging.logError(Logging.LEVEL_WARN, this, e);
            return;
        }
        removeBackupFiles(stage.dispatcher.configuration);
        stage.setLogic(BASIC);
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