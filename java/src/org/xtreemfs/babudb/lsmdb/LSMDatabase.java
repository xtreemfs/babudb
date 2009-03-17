/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.lsmdb;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xtreemfs.babudb.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.babudb.replication.Replication;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.include.common.logging.Logging;

/**
 * A LSMDatabase contains up to MAX_INDICES LSMTrees.
 * @author bjko
 */
public class LSMDatabase {

    /**
     * Maximum number of indices per database.
     */
    public static final int MAX_INDICES = 2<<8;
    
    private static final String SNAPSHOT_FILENAME_REGEXP = "IX(\\d+)V(\\d+)SEQ(\\d+)\\.idx";
    
    /**
     * The actual indices stores in LSMTrees.
     */
    private final List<LSMTree> trees;
    
    /**
     * The directori in which the database stores the LSMTree snapshots
     */
    private final String  databaseDir;
    
    /**
     * Name of this database.
     */
    private final String  databaseName;
    
    /**
     * Unique ID of the database
     */
    private final int     databaseId;
    
    /**
     * last LSN when on-disk tree was written.
     * This means that the tree has received all updates
     * (which were for that tree) including ondiskLSN.
     * This is important for recovery, because all
     * inserts with a LSN > ondiskLSN must be replayed from
     * the log.
     */
    private LSN           ondiskLSN;
    
    private final int     numIndices;
    
    private final ByteRangeComparator[] comparators;
    
    
    /**
     * Creates a new database and loads data from disk if requested.
     * @param databaseName the name of the database
     * @param databaseDir the directory in which the DB stores the snapshots
     * @param numIndices number of indices (cannot be changed)
     * @param readFromDisk true if data should be read from disk
     * @throws java.io.IOException if on-disk data cannot be read or DB directory cannot be created
     */
    public LSMDatabase(String databaseName, int databaseId, String databaseDir,
            int numIndices, boolean readFromDisk, ByteRangeComparator[] comparators) throws BabuDBException {
        
        this.numIndices = numIndices;
        this.databaseId = databaseId;
        this.databaseDir = databaseDir;
        File f = new File(this.databaseDir);
        if (!f.exists())
            f.mkdirs();
        this.databaseName = databaseName;
        this.trees = new ArrayList(numIndices);
        this.comparators = comparators;
        if (readFromDisk) {
            loadFromDisk(numIndices);
        } else {
            try {
                for (int i = 0; i < numIndices; i++) {
                    assert(comparators[i] != null);
                    trees.add(new LSMTree(null,comparators[i]));
                }
                ondiskLSN = new LSN(0,0);
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot create new index",ex);
            }
        }
    }
    
    public String[] getComparatorClassNames() {
        String[] array = new String[trees.size()];
        for (int i = 0; i< trees.size(); i++) {
            array[i] = comparators[i].getClass().getName();
        }
        return array;
    }
    
    public ByteRangeComparator[] getComparators() {
        return comparators;
    }
    
    /**
     * Load the most recent snapshots of each tree.
     * @param numIndices the number of indices to read.
     * @throws java.io.IOException if the on-disk data cannot be read
     */
    private void loadFromDisk(int numIndices) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_DEBUG, this,"loading database "+this.databaseName+" from disk...");
        for (int index = 0; index < numIndices; index++) {
            trees.add(null);
        }
        for (int index = 0; index < numIndices; index++) {
            final int idx = index;
            File f = new File(databaseDir);
            String[] files = f.list(new FilenameFilter() {

                public boolean accept(File dir, String name) {
                    return name.startsWith("IX"+idx+"V");
                }
            });
            int maxView = -1;
            int maxSeq = -1;
            Pattern p = Pattern.compile(SNAPSHOT_FILENAME_REGEXP);
            for (String fname : files) {
                Matcher m = p.matcher(fname);
                m.matches();
                Logging.logMessage(Logging.LEVEL_DEBUG, this,"inspecting snapshot: "+fname);

                int view = Integer.valueOf(m.group(2));
                int seq = Integer.valueOf(m.group(3));
                if (view > maxView) {
                    maxView = view;
                    maxSeq = seq;
                } else if (view == maxView) {
                    if (seq > maxSeq)
                        maxSeq = seq;
                }
            }
            //load max
            try {
                if (maxView > -1) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this,"loading database "+this.databaseName+" from latest snapshot:"+databaseDir+"IX"+index+"V"+maxView+"SEQ"+maxSeq);
                    assert(comparators[index] != null);
                    trees.set(index, new LSMTree(databaseDir+getSnapshotFilename(index,maxView,maxSeq),comparators[index]));
                    ondiskLSN = new LSN(maxView,maxSeq);
                } else {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this,"no snapshot for database "+this.databaseName);
                    assert(comparators[index] != null);
                    trees.set(index, new LSMTree(null,comparators[index]));
                }
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot load index from disk",ex);
            }
        }
    }
    
    /**
     * Returns the LSMTree for indexId
     * @param indexId the id of the index (0..IndexCount-1)
     * @return the LSMTree object
     */
    public LSMTree getIndex(int indexId) {
        assert((indexId >= 0) || (indexId < MAX_INDICES));
        return trees.get(indexId);
    }
    
    /**
     * Get the number of indices in this database.
     * @return the number of indices
     */
    public int getIndexCount() {
        return trees.size();
    }
    
    /**
     * Get the LSN of the current on-disk snapshot (i.e. all 
     * writes with LSN <= the on-disk LSN are in the snapshot on disk).
     * @return the LSN of the on-disk snapshot
     */
    public LSN getOndiskLSN() {
        return ondiskLSN;
    }
    
    
    /**
     * Creates a snapshot of all indices.
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
     * Writes the snapshots to disk.
     * @param viewId current viewId (i.e. of the last write)
     * @param sequenceNo current sequenceNo (i.e. of the last write)
     * @param snapIds the snapshot Ids (obtained via createSnapshot).
     * @throws java.io.IOException if a snapshot cannot be written to disk
     */
    public void writeSnapshot(int viewId, long sequenceNo, int[] snapIds) throws IOException {
        Logging.logMessage(Logging.LEVEL_INFO, this, "writing snapshot, database = " + databaseName + "...");
        for (int index = 0; index < trees.size(); index++) {
            final LSMTree tree = trees.get(index);
            final String newFileName = databaseDir+getSnapshotFilename(index,viewId,sequenceNo);
            Logging.logMessage(Logging.LEVEL_INFO, this, "snapshotting index " + index + "(dbName = " + databaseName + ")...");
            tree.materializeSnapshot(newFileName, snapIds[index]);
            Logging.logMessage(Logging.LEVEL_INFO, this, "... done (index = " + index + ", dbName = " + databaseName + ")");
        }
        Logging.logMessage(Logging.LEVEL_INFO, this, "snapshot written, database = " + databaseName);
    }
    
    public void writeSnapshot(String directory, int[] snapIds) throws IOException {
        for (int index = 0; index < trees.size(); index++) {
            final LSMTree tree = trees.get(index);
            final String newFileName = directory+"/"+getSnapshotFilename(index,0,0);
            tree.materializeSnapshot(newFileName, snapIds[index]);
        }
    }
    
    /**
     * Links the indices to the latest on-disk snapshot, cleans up any 
     * unnecessary in-memory and on-disk data
     * @param viewId the viewId of the snapshot
     * @param sequenceNo the sequenceNo of the snaphot
     * @throws java.io.IOException if snapshots cannot be cleaned up
     */
    public void cleanupSnapshot(final int viewId, final long sequenceNo) throws IOException {
        for (int index = 0; index < trees.size(); index++) {
            final LSMTree tree = trees.get(index);

            Logging.logMessage(Logging.LEVEL_INFO, this, "linking to snapshot "+databaseDir+getSnapshotFilename(index,viewId,sequenceNo) + ", dbName=" + databaseName + ", index=" + index);
            tree.linkToSnapshot(databaseDir+getSnapshotFilename(index,viewId,sequenceNo));
            Logging.logMessage(Logging.LEVEL_INFO, this, "...done");
            
            ondiskLSN = new LSN(viewId,sequenceNo);
            
            File f = new File(databaseDir);
            String[] files = f.list();
            Pattern p = Pattern.compile(SNAPSHOT_FILENAME_REGEXP);
            for (String fname : files) {
                Matcher m = p.matcher(fname);
                if (m.matches()) {
                    int fView = Integer.valueOf(m.group(2));
                    int fSeq = Integer.valueOf(m.group(3));
                    //delete snapshot if it is older (smaller LSN)
                    //than current
                    if ( (fView < viewId) ||
                         ((fView == viewId) && (fSeq < sequenceNo)) ) {
                        File snap = new File(databaseDir+fname);
                        snap.delete();
                    }
                            
                }
            }
        }
    }

    /**
     * Get the database's name.
     * @return the database's name
     */
    public String getDatabaseName() {
        return databaseName;
    }
    
    private static String getSnapshotFilename(int indexId, int viewId, long sequenceNo) {
        return "IX"+indexId+"V"+viewId+"SEQ"+sequenceNo+".idx";
    }
    
    /**
     * 
     * @param fname
     * @return the {@link LSN} retrieved from the filename.
     */
    public static LSN getSnapshotLSNbyFilename(String fname) {
        Matcher m = Pattern.compile(SNAPSHOT_FILENAME_REGEXP).matcher(fname);
        m.matches();
        
        return new LSN(Integer.valueOf(m.group(2)),Integer.valueOf(m.group(3)));
    }
    
    /**
     * @param fileName
     * @return true, if the given <code>fileName</code> matches the snapshot-filename-pattern, false otherwise.
     */
    public static boolean isSnapshotFilename(String fileName) {
        return fileName.matches(SNAPSHOT_FILENAME_REGEXP);
    }
    
    /**
     * @return a list of file details from snapshot files that can used to synchronize master and slave in replication.
     */
    public Map<String,List<Long>> getLastestSnapshotFiles() {
        Map<String,List<Long>> result = new Hashtable<String, List<Long>>();

        for (int index = 0; index < numIndices; index++) {
            final int idx = index;
            File f = new File(databaseDir);
            String[] files = f.list(new FilenameFilter() {

                public boolean accept(File dir, String name) {
                    return name.startsWith("IX"+idx+"V");
                }
            });
            
            int maxView = -1;
            int maxSeq = -1;
            Pattern p = Pattern.compile(SNAPSHOT_FILENAME_REGEXP);
            for (String fname : files) {
                Matcher m = p.matcher(fname);
                m.matches();
                Logging.logMessage(Logging.LEVEL_DEBUG, this,"inspecting snapshot: "+fname);
    
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
                    String fName = getSnapshotFilename(index,maxView,maxSeq);
                    
                    List<Long> parameter = new LinkedList<Long>();
                    File snapshotFile = new File(databaseDir + fName);
                    parameter.add(snapshotFile.length());
                    parameter.add(Replication.CHUNK_SIZE);
                    
                    result.put(databaseDir + fName, parameter);
                }
            }    
        }
        
        return result;
    }
    
    public int getDatabaseId() {
        return databaseId;
    }
}
