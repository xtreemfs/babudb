/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

import java.util.HashSet;
import java.util.Set;

import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;

/**
 * A set of configuration parameters for a new snapshot.
 * 
 * @author stender
 * 
 */
public class SnapshotConfig {
    
    private int[]      indices;
    
    private String     name;
    
    private byte[][][] prefixes;
    
    public SnapshotConfig(String snapName, int[] indices, byte[][][] prefixes) {
        this.indices = indices;
        this.name = snapName;
        this.prefixes = removeCoveringPrefixes(prefixes);
    }
    
    /**
     * Returns the name for the new snapshot.
     * 
     * @return the name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Returns an array of indices to be included in the new snapshot.
     * 
     * @return a set of indices
     */
    public int[] getIndices() {
        return indices;
    }
    
    /**
     * Returns an array of key prefixes for a given index. All records to be
     * included in the snapshot of this index have to match (exactly) one of the
     * prefixes.
     * 
     * @param index
     *            the index
     * @return the array of key prefixes
     */
    public byte[][] getPrefixes(int index) {
        return prefixes == null ? null : prefixes[index];
    }
    
    /**
     * Serializes the snapshot configuration to a buffer.
     * 
     * @return a buffer containing a binary representation of the snapshot
     *         configuration
     */
    public ReusableBuffer serialize(int dbId) {
        
        byte[] nameBytes = name.getBytes();
        int bufSize = Integer.SIZE / 8 + // dbId
            Integer.SIZE / 8 + // snapshot name length
            nameBytes.length + // snapshot name
            (indices.length + 1) * Integer.SIZE / 8 + // index list including
            // length
            Integer.SIZE / 8; // prefixes length
        
        if (prefixes != null)
            for (byte[][] prefixList : prefixes) {
                bufSize += Integer.SIZE / 8; // prefix count
                if (prefixList != null) {
                    for (byte[] bytes : prefixList)
                        bufSize += bytes.length + // prefix
                            Integer.SIZE / 8; // prefix length
                }
            }
        
        ReusableBuffer buf = BufferPool.allocate(bufSize);
        buf.putInt(dbId);
        buf.putInt(nameBytes.length);
        buf.put(nameBytes);
        
        // store the index list
        buf.putInt(indices.length);
        for (int i : indices)
            buf.putInt(i);
        
        // store the prefix matrix
        buf.putInt(prefixes == null ? 0 : prefixes.length);
        if (prefixes != null)
            for (byte[][] prefixList : prefixes) {
                buf.putInt(prefixList == null ? 0 : prefixList.length);
                if (prefixList != null)
                    for (byte[] bytes : prefixList) {
                        buf.putInt(bytes.length);
                        buf.put(bytes);
                    }
            }
        
        buf.flip();
        
        return buf;
    }
    
    /**
     * Deserializes a snapshot configuration from a buffer.
     * 
     * @param buf
     *            the buffer
     * @return the snapshot configuration
     */
    public static SnapshotConfig deserialize(ReusableBuffer buf) {
        
        buf.getInt(); // skip the dbId
        byte[] nameBytes = new byte[buf.getInt()];
        buf.get(nameBytes);
        
        // store the index list
        int[] indices = new int[buf.getInt()];
        for (int i = 0; i < indices.length; i++)
            indices[i] = buf.getInt();
        
        // store the prefix matrix
        int prefLength = buf.getInt();
        byte[][][] prefixes = prefLength == 0 ? null : new byte[prefLength][][];
        if (prefixes != null)
            for (int i = 0; i < prefixes.length; i++) {
                int len = buf.getInt();
                prefixes[i] = len == 0 ? null : new byte[len][];
                if (prefixes[i] != null)
                    for (int j = 0; j < prefixes[i].length; j++) {
                        prefixes[i][j] = new byte[buf.getInt()];
                        buf.get(prefixes[i][j]);
                    }
            }
        
        return new SnapshotConfig(new String(nameBytes), indices, prefixes);
    }
    
    /**
     * Removes all prefixes that are covered by other prefixes.
     * 
     * @param prefixes
     *            the list of input prefixes
     * @return the list of output prefixes
     */
    private byte[][][] removeCoveringPrefixes(byte[][][] prefixes) {
        
        if (prefixes == null)
            return null;
        
        byte[][][] newPrefixes = new byte[prefixes.length][][];
        for (int i = 0; i < prefixes.length; i++)
            newPrefixes[i] = removeCoveringPrefixes(prefixes[i]);
        
        return newPrefixes;
    }
    
    private byte[][] removeCoveringPrefixes(byte[][] prefixes) {
        
        if (prefixes == null)
            return null;
        
        Set<Integer> covered = new HashSet<Integer>();
        
        for (int i = 0; i < prefixes.length; i++)
            for (int j = i + 1; j < prefixes.length; j++)
                if (covers(prefixes[j], prefixes[i]))
                    covered.add(i);
                else if (covers(prefixes[i], prefixes[j]))
                    covered.add(j);
        
        int count = 0;
        byte[][] newPrefixes = new byte[prefixes.length - covered.size()][];
        for (int i = 0; i < prefixes.length; i++)
            if (!covered.contains(i))
                newPrefixes[count++] = prefixes[i];
        
        return newPrefixes;
    }
    
    private boolean covers(byte[] prefix1, byte[] prefix2) {
        
        if (prefix1.length > prefix2.length)
            return false;
        
        for (int i = 0; i < prefix1.length; i++)
            if (prefix1[i] != prefix2[i])
                return false;
        
        return true;
    }
    
}
