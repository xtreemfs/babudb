/*
 * Copyright (c) 2010, Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
 * 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this 
 * list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * Neither the name of the Konrad-Zuse-Zentrum fuer Informationstechnik Berlin 
 * nor the names of its contributors may be used to endorse or promote products 
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.transmission;

import java.io.File;
import java.io.IOException;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.interfaces.Chunk;
import org.xtreemfs.babudb.interfaces.DBFileMetaData;
import org.xtreemfs.babudb.log.DiskLogIterator;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSN;

/**
 * Interface for accessing files provided by {@link BabuDB}.
 * 
 * @author flangner
 * @since 04/14/2010
 */
public interface FileIOInterface {

    /**
     * @return the {@link DBFileMetaData} of the config file used by 
     *         {@link BabuDB}.
     */
    public DBFileMetaData getConfigFileMetaData();
    
    /**
     * Prepares the file to the given chunk.
     * 
     * @param chunk
     * @return the {@link File} to the given {@link Chunk}.
     * @throws IOException
     */
    public File getFile(Chunk chunk) throws IOException;
    
    /**
     * @param from - {@link LSN} of the first {@link LogEntry} of this iterator.
     * 
     * @throws LogEntryException
     * @throws IOException
     * 
     * @return an iterator for all locally available {@link LogEntry}s.
     */
    public DiskLogIterator getLogEntryIterator(LSN from) 
            throws LogEntryException, IOException ;

    /**
     * <p>
     * First it removes the BACKUP_LOCK_FILE, 
     * than the depreciated backupFiles.
     * </p>
     * 
     * @param configuration
     */
    public void removeBackupFiles();

    /**
     * <p>Replays the backup files, if necessary.</p>
     * 
     * @throws IOException 
     */
    public void replayBackupFiles() throws IOException;

    /**
     * <p>Makes a backup of the current files and removes them from the working 
     * directory. Writes the BACKUP_LOCK_FILE, when finished.
     * <br><br>
     * For slaves only. </p>
     * 
     * @throws IOException
     */
    public void backupFiles() throws IOException;
}