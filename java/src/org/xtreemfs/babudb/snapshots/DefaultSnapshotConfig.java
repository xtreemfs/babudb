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

/**
 * The default implementation of a snapshot configuration.
 * 
 * @author stender
 * 
 */
public class DefaultSnapshotConfig implements SnapshotConfig {
    
    private int[]      indices;
    
    private String     name;
    
    private byte[][][] prefixes;
    
    public DefaultSnapshotConfig(String snapName, int[] indices, byte[][][] prefixes) {
        this.indices = indices;
        this.name = snapName;
        this.prefixes = removeCoveringPrefixes(prefixes);
    }
    
    @Override
    public int[] getIndices() {
        return indices;
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public byte[][] getPrefixes(int index) {
        return prefixes == null? null: prefixes[index];
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
    
    public static void main(String[] args) {
        
        byte[][][] prefixes = new byte[][][] { { { 4, 5, 6 }, { 4, 5 }, { 4, 5 }, { 8 }, { 8, 3, 3 }, { 27 } } };
        DefaultSnapshotConfig cfg = new DefaultSnapshotConfig("test", new int[] { 1 }, prefixes);
        for (byte[][] plist : prefixes)
            for (byte[] ps : plist) {
                for (byte b : ps)
                    System.out.print(b);
                System.out.println();
            }
        
        System.out.println("---");
        
        for (byte[][] plist : cfg.removeCoveringPrefixes(prefixes))
            for (byte[] ps : plist) {
                for (byte b : ps)
                    System.out.print(b);
                System.out.println();
            }
        
    }
    
}
