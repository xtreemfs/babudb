/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb;

import java.io.File;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;

public class LSMTreeTest extends TestCase {
    
    private static final String SNAP_FILE  = "/tmp/snap1.bin";
    
    private static final String SNAP_FILE2 = "/tmp/snap2.bin";
    
    public LSMTreeTest() throws Exception {
    }
    
    public void setUp() {
        new File(SNAP_FILE).delete();
        new File(SNAP_FILE2).delete();
    }
    
    public void tearDown() throws Exception {
        new File(SNAP_FILE).delete();
        new File(SNAP_FILE2).delete();
    }
    
    public void testSnapshots() throws Exception {
        
        byte[][] keys = new byte[][] { "bla".getBytes(), "blub".getBytes(), "ertz".getBytes(),
            "foo".getBytes(), "yagga".getBytes() };
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
        byte[][] newKeys = new byte[][] { "gfdgf".getBytes(), "blub".getBytes(), "234".getBytes(),
            "gfdsdsg".getBytes(), "xvc".getBytes() };
        byte[][] newVals = new byte[][] { "gf".getBytes(), "werr".getBytes(), "afds".getBytes(),
            "sdaew".getBytes(), "hf".getBytes() };
        
        for (int i = 0; i < newKeys.length; i++) {
            tree.insert(newKeys[i], newVals[i]);
            map.put(newKeys[i], newVals[i]);
        }
        
        for (byte[] key : map.keySet())
            assertEquals(new String(map.get(key)), new String(tree.lookup(key)));
        
        // create, materialize and link a new snapshot
        snapId = tree.createSnapshot();
        tree.materializeSnapshot(SNAP_FILE2, snapId);
        tree.linkToSnapshot(SNAP_FILE2);
        
        for (byte[] key : map.keySet())
            assertEquals(new String(map.get(key)), new String(tree.lookup(key)));
    }
    
    public static void main(String[] args) {
        TestRunner.run(LSMTreeTest.class);
    }
    
}
