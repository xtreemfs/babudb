/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.xtreemfs.babudb.index.DefaultByteRangeComparator;

/**
 * A set of configuration parameters for a new snapshot.
 * 
 * @author stender
 * 
 */
public class DefaultSnapshotConfig implements SnapshotConfig {
    
    private int[]                           indices;
    
    private String                          name;
    
    private byte[][][]                      prefixes;
    
    private byte[][][]                      excludedPrefixes;
    
    private transient Map<Integer, Integer> indexMap;
    
    public DefaultSnapshotConfig(String snapName, int[] indices, byte[][][] prefixes,
        byte[][][] excludedPrefixes) {
        
        this.indices = indices;
        this.name = snapName;
        this.prefixes = removeCoveringPrefixes(prefixes);
        this.excludedPrefixes = excludedPrefixes;
        
        if (excludedPrefixes != null)
            for (byte[][] keys : excludedPrefixes) {
                if (keys != null)
                    Arrays.sort(keys, DefaultByteRangeComparator.getInstance());
            }
        
        initIndexMap();
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public int[] getIndices() {
        return indices;
    }
    
    @Override
    public byte[][] getPrefixes(int index) {
        
        if (indexMap == null)
            initIndexMap();
        
        return prefixes == null ? null : prefixes[indexMap.get(index)];
    }
    
    @Override
    public boolean containsKey(int index, byte[] key) {
        
        if (excludedPrefixes == null)
            return true;
        
        if (indexMap == null)
            initIndexMap();
        
        if (excludedPrefixes[indexMap.get(index)] == null)
            return true;
        
        // search for a matching prefix
        byte[][] excl = excludedPrefixes[indexMap.get(index)];
        
        int i = binarySearch(key, excl);
        
        return i >= 0;
    }
    
    private static int binarySearch(byte[] key, byte[][] prefixes) {
        int low = 0;
        int high = prefixes.length;
        
        int mid = high;
        int cmp = 0;
        
        // binary search
        while (low <= high) {
            
            mid = (low + high) >>> 1;
            byte[] currKey = prefixes[mid];
            
            cmp = startsWith(key, currKey);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        
        return -1;
    }
    
    private static int startsWith(byte[] key, byte[] prefix) {
        
        for (int i = 0; i < prefix.length; i++) {
            
            if (i >= key.length)
                return -1;
            
            if (key[i] < prefix[i])
                return 1;
            
            if (key[i] > prefix[i])
                return -1;
        }
        
        return 0;
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
    
    private void initIndexMap() {
        indexMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < indices.length; i++)
            indexMap.put(indices[i], i);
    }
    
    // public static void main(String[] args) throws Exception {
    // System.out.println(DefaultSnapshotConfig.binarySearch("mo".getBytes(),
    // new byte[][] {
    // "algo".getBytes(), "blarg".getBytes(), "mo".getBytes(), "o".getBytes(),
    // "xzu".getBytes() }));
    // }
    //    
    // /**
    // * Serializes the snapshot configuration to a buffer.
    // *
    // * @return a buffer containing a binary representation of the snapshot
    // * configuration
    // */
    // public ReusableBuffer serialize(int dbId) {
    //        
    // byte[] nameBytes = name.getBytes();
    // int bufSize = Integer.SIZE / 8 + // dbId
    // Integer.SIZE / 8 + // snapshot name length
    // nameBytes.length + // snapshot name
    // (indices.length + 1) * Integer.SIZE / 8 + // index list including
    // // length
    // Integer.SIZE / 8; // prefixes length
    //        
    // if (prefixes != null)
    // for (byte[][] prefixList : prefixes) {
    // bufSize += Integer.SIZE / 8; // prefix count
    // if (prefixList != null) {
    // for (byte[] bytes : prefixList)
    // bufSize += bytes.length + // prefix
    // Integer.SIZE / 8; // prefix length
    // }
    // }
    //        
    // ReusableBuffer buf = BufferPool.allocate(bufSize);
    // buf.putInt(dbId);
    // buf.putInt(nameBytes.length);
    // buf.put(nameBytes);
    //        
    // // store the index list
    // buf.putInt(indices.length);
    // for (int i : indices)
    // buf.putInt(i);
    //        
    // // store the prefix matrix
    // buf.putInt(prefixes == null ? 0 : prefixes.length);
    // if (prefixes != null)
    // for (byte[][] prefixList : prefixes) {
    // buf.putInt(prefixList == null ? 0 : prefixList.length);
    // if (prefixList != null)
    // for (byte[] bytes : prefixList) {
    // buf.putInt(bytes.length);
    // buf.put(bytes);
    // }
    // }
    //        
    // buf.flip();
    //        
    // return buf;
    // }
    //    
    // /**
    // * Deserializes a snapshot configuration from a buffer.
    // *
    // * @param buf
    // * the buffer
    // * @return the snapshot configuration
    // */
    // public static SnapshotConfig deserialize(ReusableBuffer buf) {
    //        
    // buf.getInt(); // skip the dbId
    // byte[] nameBytes = new byte[buf.getInt()];
    // buf.get(nameBytes);
    //        
    // // store the index list
    // int[] indices = new int[buf.getInt()];
    // for (int i = 0; i < indices.length; i++)
    // indices[i] = buf.getInt();
    //        
    // // store the prefix matrix
    // int prefLength = buf.getInt();
    // byte[][][] prefixes = prefLength == 0 ? null : new byte[prefLength][][];
    // if (prefixes != null)
    // for (int i = 0; i < prefixes.length; i++) {
    // int len = buf.getInt();
    // prefixes[i] = len == 0 ? null : new byte[len][];
    // if (prefixes[i] != null)
    // for (int j = 0; j < prefixes[i].length; j++) {
    // prefixes[i][j] = new byte[buf.getInt()];
    // buf.get(prefixes[i][j]);
    // }
    // }
    //        
    // return new SnapshotConfig(new String(nameBytes), indices, prefixes);
    // }
    
}
