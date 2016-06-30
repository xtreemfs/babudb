/*
 * Copyright (c) 2008-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.lsmdb;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.FSUtils;

/**
 * A LSMDatabase contains up to MAX_INDICES LSMTrees.
 * 
 * @author bjko
 */
public class LSMDatabase {
    
    /**
     * Maximum number of indices per database.
     */
    public static final int             MAX_INDICES              = 2 << 8;
    
    public static final LSN             NO_DB_LSN                = new LSN(0, 0);
    
    private static final String         SNAPSHOT_FILENAME_REGEXP = "IX(\\d+)V(\\d+)SEQ(\\d+)\\.idx";
    
    /**
     * The actual indices stores in LSMTrees.
     */
    private final List<LSMTree>         trees;
    
    /**
     * The directory in which the database stores the LSMTree snapshots
     */
    private final String                databaseDir;
    
    /**
     * Name of this database.
     */
    private final String                databaseName;
    
    /**
     * Unique ID of the database
     */
    private final int                   databaseId;
    
    /**
     * last LSN when on-disk tree was written. This means that the tree has
     * received all updates (which were for that tree) including ondiskLSN. This
     * is important for recovery, because all inserts with a LSN > ondiskLSN
     * must be replayed from the log.
     */
    private LSN                         ondiskLSN;
    
    private final int                   numIndices;
    
    private final ByteRangeComparator[] comparators;
    
    /**
     * enables compression of the on-disk index
     */
    private final boolean               compression;
    
    /**
     * the maximum number of entries per block in an index
     */
    private final int                   maxEntriesPerBlock;
    
    /**
     * the maximum size of an on-disk index file
     */
    private final int                   maxBlockFileSize;
    
    /**
     * disables memory-mapping of block files
     */
    private final boolean               disableMMap;
    
    /**
     * the maximum size of all block files maintained by BabuDB at which block
     * files will be mmap'ed
     */
    private final int                   mmapLimit;
    
    /**
     * Creates a new database and loads data from disk if requested.
     * 
     * @param databaseName
     *            the name of the database
     * @param databaseId
     *            the numeric database ID
     * @param databaseDir
     *            the directory in which the DB stores the checkpoints
     * @param numIndices
     *            number of indices (cannot be changed)
     * @param readFromDisk
     *            true if data should be read from disk
     * @param comparators
     *            an array containing the comparators of all indices
     * @param compression
     *            specified if compression is enabled
     * @param maxEntriesPerBlock
     *            the maximum entry count for each database block
     * @param maxBlockFileSize
     *            the maximum file size for each block file
     * @param disableMMap
     *            specified whether memory-mapping of block files is disabled
     * @param mmapLimit
     *            defines the maximum size of all databases in MB after which
     *            block files will no longer be memory-mapped
     * @throws BabuDBException
     *             if on-disk data cannot be read or DB directory cannot be
     *             created
     */
    public LSMDatabase(String databaseName, int databaseId, String databaseDir, int numIndices,
        boolean readFromDisk, ByteRangeComparator[] comparators, boolean compression, int maxEntriesPerBlock,
        int maxBlockFileSize, boolean disableMMap, int mmapLimit) throws BabuDBException {
        
        this.numIndices = numIndices;
        this.databaseId = databaseId;
        File f = new File(databaseDir);
        if (!f.exists())
            f.mkdirs();
        this.databaseDir = f.getAbsolutePath();
        
        this.databaseName = databaseName;
        this.trees = new ArrayList<LSMTree>(numIndices);
        this.comparators = comparators;
        this.compression = compression;
        this.maxEntriesPerBlock = maxEntriesPerBlock;
        this.maxBlockFileSize = maxBlockFileSize;
        this.disableMMap = disableMMap;
        this.mmapLimit = mmapLimit;
        
        if (readFromDisk) {
            loadFromDisk(numIndices);
        } else {
            try {
                for (int i = 0; i < numIndices; i++) {
                    assert (comparators[i] != null);
                    trees.add(new LSMTree(null, comparators[i], this.compression, maxEntriesPerBlock,
                        maxBlockFileSize, !disableMMap, mmapLimit));
                }
                ondiskLSN = NO_DB_LSN;
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot create new index", ex);
            }
        }
    }
    
    public String[] getComparatorClassNames() {
        String[] array = new String[trees.size()];
        for (int i = 0; i < trees.size(); i++) {
            array[i] = comparators[i].getClass().getName();
        }
        return array;
    }
    
    public ByteRangeComparator[] getComparators() {
        return comparators;
    }
    
    /**
     * Load the most recent snapshots of each tree.
     * 
     * @param numIndices
     *            the number of indices to read.
     * @throws java.io.IOException
     *             if the on-disk data cannot be read
     */
    private void loadFromDisk(int numIndices) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "loading database " + this.databaseName
            + " from disk...");
        for (int index = 0; index < numIndices; index++) {
            trees.add(null);
        }
        for (int index = 0; index < numIndices; index++) {
            final int idx = index;
            File f = new File(databaseDir);
            String[] files = f.list(new FilenameFilter() {
                
                public boolean accept(File dir, String name) {
                    return name.startsWith("IX" + idx + "V");
                }
            });
            if (files == null)
                throw new BabuDBException(ErrorCode.IO_ERROR, "database directory '" + databaseDir
                    + "' does not exist");
            
            int maxView = -1;
            long maxSeq = -1;
            Pattern p = Pattern.compile(SNAPSHOT_FILENAME_REGEXP);
            for (String fname : files) {
                Matcher m = p.matcher(fname);
                m.matches();
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "inspecting snapshot: " + fname);
                
                int view = Integer.valueOf(m.group(2));
                long seq = Long.valueOf(m.group(3));
                if (view > maxView) {
                    maxView = view;
                    maxSeq = seq;
                } else if (view == maxView) {
                    if (seq > maxSeq)
                        maxSeq = seq;
                }
            }
            // load max
            try {
                if (maxView > -1) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "loading database " + this.databaseName
                        + " from latest snapshot:" + databaseDir + File.separator + "IX" + index + "V"
                        + maxView + "SEQ" + maxSeq);
                    assert (comparators[index] != null);
                    trees.set(index, new LSMTree(databaseDir + File.separator
                        + getSnapshotFilename(index, maxView, maxSeq), comparators[index], this.compression,
                        this.maxEntriesPerBlock, this.maxBlockFileSize, !this.disableMMap, this.mmapLimit));
                    ondiskLSN = new LSN(maxView, maxSeq);
                } else {
                    ondiskLSN = NO_DB_LSN;
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "no snapshot for database "
                        + this.databaseName);
                    assert (comparators[index] != null);
                    trees.set(index, new LSMTree(null, comparators[index], this.compression,
                        this.maxEntriesPerBlock, this.maxBlockFileSize, !this.disableMMap, this.mmapLimit));
                }
            } catch (IOException ex) {
                Logging.logError(Logging.LEVEL_ERROR, this, ex);
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot load index from disk", ex);
            }
        }
    }
    
    /**
     * Returns the LSMTree for indexId
     * 
     * @param indexId
     *            the id of the index (0..IndexCount-1)
     * @return the LSMTree object
     */
    public LSMTree getIndex(int indexId) {
        assert ((indexId >= 0) || (indexId < MAX_INDICES));
        return trees.get(indexId);
    }
    
    /**
     * Get the number of indices in this database.
     * 
     * @return the number of indices
     */
    public int getIndexCount() {
        return trees.size();
    }
    
    /**
     * Get the LSN of the current on-disk snapshot (i.e. all writes with LSN <=
     * the on-disk LSN are in the snapshot on disk).
     * 
     * @return the LSN of the on-disk snapshot
     */
    public LSN getOndiskLSN() {
        return ondiskLSN;
    }
    
    /**
     * Creates a snapshot of all indices.
     * 
     * @return a list with snapshot Ids for each index
     */
    public int[] createSnapshot() {
        int[] snapIds = new int[trees.size()];
        for (int index = 0; index < trees.size(); index++) {
            final LSMTree tree = trees.get(index);
            snapIds[index] = tree.createSnapshot();
        }
        return snapIds;
    }
    
    /**
     * Creates a snapshot of a given set of indices.
     * 
     * @return a list with snapshot Ids for each index
     */
    public int[] createSnapshot(int[] indices) {
        int[] snapIds = new int[indices.length];
        for (int index = 0; index < indices.length; index++) {
            final LSMTree tree = trees.get(indices[index]);
            snapIds[index] = tree.createSnapshot();
        }
        return snapIds;
    }
    
    /**
     * Writes the snapshots to disk.
     * 
     * @param viewId
     *            current viewId (i.e. of the last write)
     * @param sequenceNo
     *            current sequenceNo (i.e. of the last write)
     * @param snapIds
     *            the snapshot Ids (obtained via createSnapshot).
     * @throws java.io.IOException
     *             if a snapshot cannot be written to disk
     */
    public void writeSnapshot(int viewId, long sequenceNo, int[] snapIds) throws IOException {
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "writing snapshot, database = " + databaseName + "...");
        for (int index = 0; index < trees.size(); index++) {
            
            final LSMTree tree = trees.get(index);
            
            if (Logging.isInfo())
                Logging.logMessage(Logging.LEVEL_INFO, this, "snapshotting index " + index + "(dbName = "
                    + databaseName + ")...");
            
            File tmpDir = new File(databaseDir, ".currentSnapshot");
            File targetDir = new File(databaseDir, getSnapshotFilename(index, viewId, sequenceNo));
            
            if (targetDir.exists()) {
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "skipping index'" + index
                    + ", as a valid checkpoint (" + targetDir + ") exists already");
                continue;
            }
            
            // clean up incomplete old checkpoints if necessary
            if (tmpDir.exists())
                FSUtils.delTree(tmpDir);
            
            tree.materializeSnapshot(tmpDir.getAbsolutePath(), snapIds[index]);
            
            if (!tmpDir.renameTo(targetDir))
                throw new IOException("could not rename '" + tmpDir + "' to " + targetDir);
            
            if (Logging.isInfo())
                Logging.logMessage(Logging.LEVEL_INFO, this, "... done (index = " + index + ", dbName = "
                    + databaseName + ")");
        }
        if (Logging.isInfo())
            Logging.logMessage(Logging.LEVEL_INFO, this, "snapshot written, database = " + databaseName);
    }
    
    public void writeSnapshot(String directory, int[] snapIds, int viewId, long sequenceNumber)
        throws IOException {
        
        for (int index = 0; index < trees.size(); index++) {
            final LSMTree tree = trees.get(index);
            final String newFileName = directory + "/" + getSnapshotFilename(index, viewId, sequenceNumber);
            tree.materializeSnapshot(newFileName, snapIds[index]);
        }
    }
    
    public void writeSnapshot(String directory, int[] snapIds, SnapshotConfig cfg) throws IOException {
        
        for (int i = 0; i < cfg.getIndices().length; i++) {
            
            int index = cfg.getIndices()[i];
            
            final LSMTree tree = trees.get(index);
            final String newFileName = directory + "/" + getSnapshotFilename(index, 0, 0);
            
            File dir = new File(directory);
            if (!dir.exists() && !dir.mkdirs())
                throw new IOException("Directory doesnt exist and cannot be created:'" + directory + "'");
            
            tree.materializeSnapshot(newFileName, snapIds[i], index, cfg);
        }
    }
    
    /**
     * Links the indices to the latest on-disk snapshot, cleans up any
     * unnecessary in-memory and on-disk data
     * 
     * @param viewId
     *            the viewId of the snapshot
     * @param sequenceNo
     *            the sequenceNo of the snaphot
     * @throws java.io.IOException
     *             if snapshots cannot be cleaned up
     */
    public void cleanupSnapshot(final int viewId, final long sequenceNo) throws IOException {
        
        for (int index = 0; index < trees.size(); index++) {
            
            final LSMTree tree = trees.get(index);
            
            Logging.logMessage(Logging.LEVEL_INFO, this, "linking to snapshot " + databaseDir
                + File.separator + getSnapshotFilename(index, viewId, sequenceNo) + ", dbName="
                + databaseName + ", index=" + index);
            
            // catch any I/O exception that may occur while re-linking the
            // snapshot; this is done to ensure that old checkpoints are
            // properly cleaned up, and the database remains in a consistent
            // state
            IOException exception = null;
            try {
                tree.linkToSnapshot(databaseDir + File.separator
                    + getSnapshotFilename(index, viewId, sequenceNo));
            } catch (ClosedByInterruptException exc) {
                Logging.logError(Logging.LEVEL_DEBUG, this, exc);
            } catch (IOException exc) {
                Logging.logError(Logging.LEVEL_ERROR, this, exc);
                exception = exc;
            }
            Logging.logMessage(Logging.LEVEL_INFO, this, "...done");
            
            ondiskLSN = new LSN(viewId, sequenceNo);
            
            File f = new File(databaseDir);
            String[] files = f.list();
            Pattern p = Pattern.compile(SNAPSHOT_FILENAME_REGEXP);
            for (String fname : files) {
                Matcher m = p.matcher(fname);
                if (m.matches()) {
                    int fView = Integer.valueOf(m.group(2));
                    int fSeq = Integer.valueOf(m.group(3));
                    // delete snapshot if it is older (smaller LSN)
                    // than current
                    if ((fView < viewId) || ((fView == viewId) && (fSeq < sequenceNo))) {
                        File snap = new File(databaseDir + File.separator + fname);
                        if (snap.isDirectory())
                            FSUtils.delTree(snap);
                        else
                            snap.delete();
                    }
                    
                }
            }
            
            // throw any I/O exception that has occurred before
            if (exception != null)
                throw new IOException(exception);
        }
    }
    
    /**
     * Get the database's name.
     * 
     * @return the database's name
     */
    public String getDatabaseName() {
        return databaseName;
    }
    
    public static String getSnapshotFilename(int indexId, int viewId, long sequenceNo) {
        return "IX" + indexId + "V" + viewId + "SEQ" + sequenceNo + ".idx";
    }
    
    /**
     * 
     * @param fname
     * @return the {@link LSN} retrieved from the filename.
     */
    public static LSN getSnapshotLSNbyFilename(String fname) {
        Matcher m = Pattern.compile(SNAPSHOT_FILENAME_REGEXP).matcher(new File(fname).getName());
        m.matches();
        
        return new LSN(Integer.valueOf(m.group(2)), Integer.valueOf(m.group(3)));
    }
    
    /**
     * @param fileName
     * @return true, if the given <code>fileName</code> matches the
     *         snapshot-filename-pattern, false otherwise.
     */
    public static boolean isSnapshotFilename(String fileName) {
        return new File(fileName).getName().matches(SNAPSHOT_FILENAME_REGEXP);
    }
    
    /**
     * @return a list of file details from snapshot files that can used to
     *         synchronize master and slave in replication.
     */
    public ArrayList<DBFileMetaData> getLastestSnapshotFiles() {
        ArrayList<DBFileMetaData> result = new ArrayList<DBFileMetaData>();
        
        for (int index = 0; index < numIndices; index++) {
            final int idx = index;
            File f = new File(databaseDir);
            String[] files = f.list(new FilenameFilter() {
                
                public boolean accept(File dir, String name) {
                    return name.startsWith("IX" + idx + "V");
                }
            });
            
            int maxView = -1;
            int maxSeq = -1;
            Pattern p = Pattern.compile(SNAPSHOT_FILENAME_REGEXP);
            for (String fname : files) {
                Matcher m = p.matcher(fname);
                m.matches();
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "inspecting snapshot: " + fname);
                
                int view = Integer.valueOf(m.group(2));
                int seq = Integer.valueOf(m.group(3));
                if (view > maxView) {
                    maxView = view;
                    maxSeq = seq;
                } else if (view == maxView) {
                    if (seq > maxSeq)
                        maxSeq = seq;
                }
                
                if (maxView > -1) {
                    String fName = getSnapshotFilename(index, maxView, maxSeq);
                    File snapshotDir = new File(databaseDir + File.separator + fName);
                    
                    if (snapshotDir.isDirectory()) {
                        for (File file : snapshotDir.listFiles()) {
                            result.add(new DBFileMetaData(databaseDir + File.separator + fName
                                + File.separator + file.getName(), file.length()));
                        }
                    } else {
                        // for compatibility with older versions of BabuDB
                        result.add(new DBFileMetaData(databaseDir + File.separator + fName, snapshotDir
                                .length()));
                    }
                }
            }
        }
        
        return result;
    }
    
    public int getDatabaseId() {
        return databaseId;
    }
    
    /**
     * @author flangner
     * @since 01/04/2011
     */
    public final static class DBFileMetaData {
        
        public final String file;
        
        public final long   size;
        
        public DBFileMetaData(String file, long size) {
            this.file = file;
            this.size = size;
        }
    }
}
