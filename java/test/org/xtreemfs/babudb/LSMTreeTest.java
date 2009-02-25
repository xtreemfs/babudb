/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb;

import java.io.File;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.include.common.logging.Logging;

public class LSMTreeTest extends TestCase {
    
    private static final String SNAP_FILE  = "/tmp/snap1.bin";
    
    private static final String SNAP_FILE2 = "/tmp/snap2.bin";
    
    private static final String SNAP_FILE3 = "/tmp/snap3.bin";
    
    public LSMTreeTest() throws Exception {
    }
    
    public void setUp() {
        Logging.start(Logging.LEVEL_ERROR);
        new File(SNAP_FILE).delete();
        new File(SNAP_FILE2).delete();
        new File(SNAP_FILE3).delete();
    }
    
    public void tearDown() throws Exception {
        new File(SNAP_FILE).delete();
        new File(SNAP_FILE2).delete();
        new File(SNAP_FILE3).delete();
    }
    
    public void testSnapshots() throws Exception {
        
        byte[][] keys = new byte[][] { "00001".getBytes(), "00002".getBytes(), "00003".getBytes(),
            "00004".getBytes(), "00005".getBytes() };
        byte[][] vals = new byte[][] { "1".getBytes(), "2".getBytes(), "3".getBytes(),
            "4".getBytes(), "5".getBytes() };
        
        LSMTree tree = new LSMTree(null, new DefaultByteRangeComparator());
        SortedMap<byte[], byte[]> map = new TreeMap<byte[], byte[]>(
            new DefaultByteRangeComparator());
        
        // insert some key-value pairs
        for (int i = 0; i < keys.length; i++) {
            tree.insert(keys[i], vals[i]);
            map.put(keys[i], vals[i]);
        }
        
        for (byte[] key : map.keySet())
            assertEquals(new String(map.get(key)), new String(tree.lookup(key)));
        
        // create, materialize and link a new snapshot
        int snapId = tree.createSnapshot();
        tree.materializeSnapshot(SNAP_FILE, snapId);
        tree.linkToSnapshot(SNAP_FILE);
        
        for (byte[] key : map.keySet())
            assertEquals(new String(map.get(key)), new String(tree.lookup(key)));
        
        // insert some more keys-value pairs
        byte[][] newKeys = new byte[][] { "00006".getBytes(), "00002".getBytes(),
            "00007".getBytes(), "00008".getBytes(), "00009".getBytes(), "00003".getBytes() };
        byte[][] newVals = new byte[][] { "gf".getBytes(), "werr".getBytes(), "afds".getBytes(),
            "sdaew".getBytes(), "hf".getBytes(), null };
        
        for (int i = 0; i < newKeys.length; i++) {
            tree.insert(newKeys[i], newVals[i]);
            if (newVals[i] != null)
                map.put(newKeys[i], newVals[i]);
            else
                map.remove(newKeys[i]);
        }
        
        for (byte[] key : map.keySet())
            assertEquals(new String(map.get(key)), new String(tree.lookup(key)));
        
        // perform a prefix lookup
        Iterator<Entry<byte[], byte[]>> it = tree.prefixLookup(new byte[0]);
        Iterator<Entry<byte[], byte[]>> entries = map.entrySet().iterator();
        
        int count = 0;
        for (; it.hasNext(); count++) {
            
            Entry<byte[], byte[]> entry = it.next();
            Entry<byte[], byte[]> mapEntry = entries.next();
            
            assertEquals(new String(entry.getKey()), new String(mapEntry.getKey()));
            assertEquals(new String(entry.getValue()), new String(mapEntry.getValue()));
        }
        
        assertEquals(map.size(), count);
        
        // create, materialize and link a new snapshot
        snapId = tree.createSnapshot();
        tree.materializeSnapshot(SNAP_FILE2, snapId);
        tree.linkToSnapshot(SNAP_FILE2);
        
        for (byte[] key : map.keySet())
            assertEquals(new String(map.get(key)), new String(tree.lookup(key)));
        
        // delete all entries
        for (byte[] key : keys) {
            map.remove(key);
            tree.delete(key);
        }
        
        for (byte[] key : newKeys) {
            map.remove(key);
            tree.delete(key);
        }
        
        assertEquals(0, map.size());
        assertFalse(tree.prefixLookup(new byte[0]).hasNext());
        
        // create, materialize and link a new snapshot
        snapId = tree.createSnapshot();
        tree.materializeSnapshot(SNAP_FILE3, snapId);
        tree.linkToSnapshot(SNAP_FILE3);
        
        it = tree.prefixLookup(new byte[0]);
        assertFalse(it.hasNext());
    }
    
    public static void main(String[] args) {
        TestRunner.run(LSMTreeTest.class);
    }
    
}
