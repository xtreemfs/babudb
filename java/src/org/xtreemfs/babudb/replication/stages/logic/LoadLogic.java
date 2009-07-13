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
import java.util.ArrayList;
import java.util.List;

import org.xtreemfs.babudb.interfaces.Chunk;
import org.xtreemfs.babudb.interfaces.DBFileMetaData;
import org.xtreemfs.babudb.interfaces.DBFileMetaDataSet;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponseAvailableListener;

import static org.xtreemfs.babudb.replication.DirectFileIO.*;

/**
 * <p>Perform a initial load request at the master.</p>
 * 
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
        return LogicID.LOAD;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() throws Exception{
        // backup the old dbs and stop the heartBeat
        stage.dispatcher.heartbeat.infarction();
        backupFiles(stage.dispatcher.configuration);
        
        final List<Chunk> chunks = new ArrayList<Chunk>();
        RPCResponse<DBFileMetaDataSet> rp = stage.dispatcher.master.load(stage.loadUntil);

        LSN fromSnapshot = null;
        // make chunks
        DBFileMetaDataSet result = rp.get();  
        
        // switch log file
        if (result == null) {
            stage.dispatcher.db.logger.switchLogFile(true);
            stage.setCurrentLogic(LogicID.BASIC);
            return;
        }
        
        for (DBFileMetaData fileData : result) {
            
            // validate the informations
            String fileName = fileData.getFileName();
            long fileSize = fileData.getFileSize();
            long maxChunkSize = fileData.getMaxChunkSize();
            assert (fileSize > 0L) : "Empty files are not allowed.";
            assert (maxChunkSize > 0L) : "Empty chunks are not allowed.";

            // get the expected latest LSN for the end of the loading process
            if (fromSnapshot == null && LSMDatabase.isSnapshotFilename(fileName))
                fromSnapshot = LSMDatabase.getSnapshotLSNbyFilename(fileName);
            
            // calculate chunks, request them and add them to the list
            long begin = 0L;
            for (long end = maxChunkSize; end < fileSize; end += maxChunkSize) {
                final Chunk chunk = new Chunk(fileName, begin, end);
                chunks.add(chunk);
                begin = end;
                
                // request the chunk
                RPCResponse<ReusableBuffer> chunkRp = stage.dispatcher.master.chunk(chunk);           
                chunkRp.registerListener(new RPCResponseAvailableListener<ReusableBuffer>() {
                
                    @Override
                    public void responseAvailable(RPCResponse<ReusableBuffer> r) {
                        try {
                            // insert the chunk
                            ReusableBuffer buffer = r.get();
                            
                            int length = (int) (chunk.getEnd() - chunk.getBegin());
                            FileOutputStream fO = null;
                            try {
                                // insert the file input
                                fO = new FileOutputStream(getFile(chunk));
                                fO.write(buffer.array(), (int) chunk.getBegin(),length);
                            } catch (Exception e) {
                                // make a new initial load
                                stage.interrupt();
                            } finally {
                                if (fO!=null) fO.close();
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
                        }
                    }
                });
            }        
        }
        
        try {
            // wait for the last response
            synchronized (chunks) {
                if (!chunks.isEmpty())
                    chunks.wait();
            }

            // reload the dbs
            stage.dispatcher.db.reset(fromSnapshot);
            stage.dispatcher.updateLatestLSN(fromSnapshot);
            stage.setCurrentLogic(LogicID.BASIC);
            removeBackupFiles(stage.dispatcher.configuration);
            
        } catch (InterruptedException e) {
            // retry the initial load
            Logging.logMessage(Logging.LEVEL_ERROR, stage, "Initial load was interrupted!");
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

        if (LSMDatabase.isSnapshotFilename(fName)) {
            // create the db-name directory, if necessary
            new File(stage.dispatcher.db.configuration.getBaseDir()
                    + chnk.getParentFile().getName() + File.separatorChar)
                    .mkdirs();
            // create the file if necessary
            result = new File(stage.dispatcher.db.configuration.getBaseDir()
                    + chnk.getParentFile().getName() + File.separatorChar
                    + fName);
            result.createNewFile();
        } else if (chnk.getParent() == null) {
            // create the file if necessary
            result = new File(stage.dispatcher.db.configuration.getBaseDir()+
                    stage.dispatcher.db.configuration.getDbCfgFile());
            result.createNewFile();
        } else {
            // create the file if necessary
            result = new File(stage.dispatcher.db.configuration.getDbLogDir()
                    + fName);
            result.createNewFile();
        }
        return result;
    }
}