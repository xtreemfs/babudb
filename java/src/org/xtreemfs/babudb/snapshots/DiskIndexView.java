/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.reader.DiskIndex;

/**
 * This class provides simple read-only access to all immutable on-disk indices
 * of a BabuDB database.
 * 
 * @author stender
 * 
 */
public class DiskIndexView implements BabuDBView {
    
    private Map<Integer, DiskIndex> indexMap;
    
    /**
     * Creates a new <code>DiskIndexBabuDBImpl</code>.
     * 
     * @param dir
     *            the directory containing the indices
     * @param comps
     *            an array of comparators for ALL indices in the database (i.e.
     *            also those of which no snapshots have been taken)
     * @throws BabuDBException
     *             if an error occurred during the initialization
     */
    public DiskIndexView(String dir, ByteRangeComparator[] comps) throws BabuDBException {
        
        try {
            
            indexMap = new HashMap<Integer, DiskIndex>();
            
            File dirAsFile = new File(dir);
            
            String[] files = dirAsFile.list(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.endsWith(".idx");
                }
            });
            
            for (String file : files) {
                int index = Integer.parseInt(file.substring(file.indexOf("IX") + 2, file.indexOf('V')));
                indexMap.put(index, new DiskIndex(dir + "/" + file, comps[index]));
            }
            
        } catch (IOException exc) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "could not load index file", exc);
        }
    }
    
    @Override
    public byte[] directLookup(int indexId, byte[] key) throws BabuDBException {
        
        DiskIndex index = indexMap.get(indexId);
        if (index == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + indexId + " does not exist");
        
        return index.lookup(key);
    }
    
    @Override
    public Iterator<Entry<byte[], byte[]>> directPrefixLookup(int indexId, byte[] key) throws BabuDBException {
        
        DiskIndex index = indexMap.get(indexId);
        if (index == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + indexId + " does not exist");
        
        byte[][] range = index.getComparator().prefixToRange(key, true);
        return index.rangeLookup(range[0], range[1], true);
    }
    
    @Override
    public void shutdown() throws BabuDBException {
        try {
            for (DiskIndex index : indexMap.values())
                index.destroy();
        } catch (IOException exc) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "", exc);
        }
    }
    
}
