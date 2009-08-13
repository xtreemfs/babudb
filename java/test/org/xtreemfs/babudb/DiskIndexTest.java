/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.xtreemfs.babudb.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.reader.DiskIndex;
import org.xtreemfs.babudb.index.writer.DiskIndexWriter;
import org.xtreemfs.include.common.logging.Logging;

import junit.framework.TestCase;
import junit.textui.TestRunner;

public class DiskIndexTest extends TestCase {
    
    private static final String              PATH1             = "/tmp/index1.bin";
    
    private static final String              PATH2             = "/tmp/index2.bin";
    
    private static final int                 MAX_BLOCK_ENTRIES = 16;
    
    private static final int                 NUM_ENTRIES       = 50000;
    
    private static final ByteRangeComparator COMP              = new DefaultByteRangeComparator();
    
    private static final boolean             COMPRESSED        = true;
    
    public void setUp() throws Exception {
        Logging.start(Logging.LEVEL_ERROR);
    }
    
    public void tearDown() throws Exception {
        new File(PATH1).delete();
        new File(PATH2).delete();
    }
    
    public void testLookup() throws Exception {
        
        // create a new disk index and look up each key
        byte[][] entries = { new byte[] { '\0' }, "word".getBytes(), new byte[] { '#' } };
        populateDiskIndex(entries);
        DiskIndex diskIndex = new DiskIndex(PATH2, new DefaultByteRangeComparator(), COMPRESSED);
        
        for (byte[] entry : entries)
            assertEquals(0, COMP.compare(entry, diskIndex.lookup(entry)));
        assertEquals(entries.length, diskIndex.numKeys());
        diskIndex.destroy();
        
        // create an empty disk index
        entries = new byte[][] {};
        populateDiskIndex(entries);
        diskIndex = new DiskIndex(PATH2, new DefaultByteRangeComparator(), COMPRESSED);
        
        assertEquals(entries.length, diskIndex.numKeys());
        diskIndex.destroy();
        
        // create a disk index with a single entry and look up the key
        entries = new byte[][] { "test".getBytes() };
        populateDiskIndex(entries);
        diskIndex = new DiskIndex(PATH2, new DefaultByteRangeComparator(), COMPRESSED);
        
        for (byte[] entry : entries)
            assertEquals(0, COMP.compare(entry, diskIndex.lookup(entry)));
        assertEquals(entries.length, diskIndex.numKeys());
        diskIndex.destroy();
        
        // create another disk index and look up each key
        entries = new byte[][] { "fdsaf".getBytes(), "blubberbla".getBytes(), "zzz".getBytes(),
            "aaa".getBytes(), "aab".getBytes() };
        populateDiskIndex(entries);
        diskIndex = new DiskIndex(PATH2, new DefaultByteRangeComparator(), COMPRESSED);
        
        for (byte[] entry : entries)
            assertEquals(0, COMP.compare(entry, diskIndex.lookup(entry)));
        assertEquals(entries.length, diskIndex.numKeys());
        diskIndex.destroy();
    }
    
    public void estCompleteLookup() throws Exception {
        
        // initialize a map w/ random strings
        SortedMap<byte[], byte[]> map = new TreeMap<byte[], byte[]>(COMP);
        for (int i = 0; i < NUM_ENTRIES; i++)
            map.put(createRandomString(1, 15).getBytes(), createRandomString(1, 15).getBytes());
        
        // delete old index file
        new File(PATH1).delete();
        
        // write the map to a disk index
        DiskIndexWriter index = new DiskIndexWriter(PATH1, MAX_BLOCK_ENTRIES, COMPRESSED);
        index.writeIndex(map.entrySet().iterator());
        
        // read the disk index
        DiskIndex diskIndex = new DiskIndex(PATH1, new DefaultByteRangeComparator(), COMPRESSED);
        
        // look up each element
        Iterator<Entry<byte[], byte[]>> it = map.entrySet().iterator();
        // long t0 = System.currentTimeMillis();
        // int count = 0;
        while (it.hasNext()) {
            // count++;
            Entry<byte[], byte[]> next = it.next();
            byte[] result = diskIndex.lookup(next.getKey());
            assertEquals(0, COMP.compare(result, next.getValue()));
            // if (count % 1000 == 0)
            // System.out.println(count);
        }
        // long time = System.currentTimeMillis() - t0;
        // System.out.println(time + " ms");
        // System.out.println(count * 1000 / time + " lookups/s");
        
        diskIndex.destroy();
    }
    
    public void estPrefixLookup() throws Exception {
        
        final String[] keys = { "bla", "brabbel", "foo", "kfdkdkdf", "ouuou", "yagga", "yyy", "z" };
        final String[] vals = { "blub", "233222", "bar", "34", "nnnnn", "ertz", "zzzzzz", "wuerg" };
        
        SortedMap<byte[], byte[]> testMap = new TreeMap<byte[], byte[]>(COMP);
        for (int i = 0; i < keys.length; i++)
            testMap.put(keys[i].getBytes(), vals[i].getBytes());
        
        // write the map to a disk index
        new File(PATH2).delete();
        DiskIndexWriter index = new DiskIndexWriter(PATH2, 4, COMPRESSED);
        index.writeIndex(testMap.entrySet().iterator());
        
        // read the disk index
        DiskIndex diskIndex = new DiskIndex(PATH2, new DefaultByteRangeComparator(), COMPRESSED);
        
        // create an iterator w/ matching start and end buffers
        Iterator<Entry<byte[], byte[]>> it = diskIndex.rangeLookup(keys[1].getBytes(), keys[5].getBytes(),
            true);
        for (int i = 1; i < 5; i++) {
            Entry<byte[], byte[]> entry = it.next();
            assertEquals(keys[i], new String(entry.getKey()));
            assertEquals(vals[i], new String(entry.getValue()));
        }
        
        assertFalse(it.hasNext());
        
        // create an iterator w/o matching start and end buffers
        it = diskIndex.rangeLookup("blu".getBytes(), "yyz".getBytes(), true);
        for (int i = 1; i < 7; i++) {
            Entry<byte[], byte[]> entry = it.next();
            assertEquals(keys[i], new String(entry.getKey()));
            assertEquals(vals[i], new String(entry.getValue()));
        }
        
        assertFalse(it.hasNext());
        
        // check ranges outside the boundaries; should be empty
        it = diskIndex.rangeLookup("A".getBytes(), "Z".getBytes(), true);
        assertFalse(it.hasNext());
        
        it = diskIndex.rangeLookup("1".getBytes(), "2".getBytes(), true);
        assertFalse(it.hasNext());
        
        // create a disk index from an empty index file
        index = new DiskIndexWriter(PATH1, 4, COMPRESSED);
        index.writeIndex(new HashMap().entrySet().iterator());
        
        diskIndex = new DiskIndex(PATH1, new DefaultByteRangeComparator(), COMPRESSED);
        
        // check ranges; should all be empty
        it = diskIndex.rangeLookup(new byte[0], new byte[0], true);
        assertFalse(it.hasNext());
        
        it = diskIndex.rangeLookup(null, null, true);
        assertFalse(it.hasNext());
        
        it = diskIndex.rangeLookup("b".getBytes(), null, true);
        assertFalse(it.hasNext());
        
        it = diskIndex.rangeLookup(null, "x".getBytes(), true);
        assertFalse(it.hasNext());
        
        it = diskIndex.rangeLookup("b".getBytes(), "x".getBytes(), true);
        assertFalse(it.hasNext());
    }
    
    public void estDescendingPrefixLookup() throws Exception {
        
        final String[] keys = { "bla", "brabbel", "foo", "kfdkdkdf", "ouuou", "yagga", "yyy", "z" };
        final String[] vals = { "blub", "233222", "bar", "34", "nnnnn", "ertz", "zzzzzz", "wuerg" };
        
        TreeMap<byte[], byte[]> testMap = new TreeMap<byte[], byte[]>(COMP);
        for (int i = 0; i < keys.length; i++)
            testMap.put(keys[i].getBytes(), vals[i].getBytes());
        
        // write the map to a disk index
        new File(PATH2).delete();
        DiskIndexWriter index = new DiskIndexWriter(PATH2, 4, COMPRESSED);
        index.writeIndex(testMap.entrySet().iterator());
        
        // read the disk index
        DiskIndex diskIndex = new DiskIndex(PATH2, new DefaultByteRangeComparator(), COMPRESSED);
        
        // create an iterator w/ matching start and end buffers
        Iterator<Entry<byte[], byte[]>> it = diskIndex.rangeLookup(keys[1].getBytes(), keys[5].getBytes(),
            false);
        for (int i = 4; i > 0; i--) {
            Entry<byte[], byte[]> entry = it.next();
            assertEquals(keys[i], new String(entry.getKey()));
            assertEquals(vals[i], new String(entry.getValue()));
        }
        
        assertFalse(it.hasNext());
        
        // create an iterator w/o matching start and end buffers
        it = diskIndex.rangeLookup("blu".getBytes(), "yyz".getBytes(), false);
        for (int i = 6; i > 0; i--) {
            Entry<byte[], byte[]> entry = it.next();
            assertEquals(keys[i], new String(entry.getKey()));
            assertEquals(vals[i], new String(entry.getValue()));
        }
        
        assertFalse(it.hasNext());
        
        // check ranges outside the boundaries; should be empty
        it = diskIndex.rangeLookup("A".getBytes(), "Z".getBytes(), false);
        assertFalse(it.hasNext());
        
        it = diskIndex.rangeLookup("1".getBytes(), "2".getBytes(), false);
        assertFalse(it.hasNext());
        
        // create a disk index from an empty index file
        index = new DiskIndexWriter(PATH1, 4, COMPRESSED);
        index.writeIndex(new HashMap().entrySet().iterator());
        
        diskIndex = new DiskIndex(PATH1, new DefaultByteRangeComparator(), COMPRESSED);
        
        // check ranges; should all be empty
        it = diskIndex.rangeLookup(new byte[0], new byte[0], false);
        assertFalse(it.hasNext());
        
        it = diskIndex.rangeLookup(null, null, false);
        assertFalse(it.hasNext());
        
        it = diskIndex.rangeLookup("b".getBytes(), null, false);
        assertFalse(it.hasNext());
        
        it = diskIndex.rangeLookup(null, "x".getBytes(), false);
        assertFalse(it.hasNext());
        
        it = diskIndex.rangeLookup("b".getBytes(), "x".getBytes(), false);
        assertFalse(it.hasNext());
    }
    
    public void estLargeScalePrefixLookup() throws Exception {
        
        // initialize a map w/ random strings
        SortedMap<byte[], byte[]> map = new TreeMap<byte[], byte[]>(COMP);
        for (int i = 0; i < NUM_ENTRIES; i++)
            map.put(createRandomString(1, 15).getBytes(), createRandomString(1, 15).getBytes());
        
        // delete old index file
        new File(PATH1).delete();
        
        // write the map to a disk index
        DiskIndexWriter index = new DiskIndexWriter(PATH1, MAX_BLOCK_ENTRIES, COMPRESSED);
        index.writeIndex(map.entrySet().iterator());
        
        // read the disk index
        DiskIndex diskIndex = new DiskIndex(PATH1, new DefaultByteRangeComparator(), COMPRESSED);
        
        {
            // look up the complete list of elements
            Iterator<Entry<byte[], byte[]>> mapIt = map.entrySet().iterator();
            Iterator<Entry<byte[], byte[]>> indexIt = diskIndex.rangeLookup(null, null, true);
            
            while (indexIt.hasNext() || mapIt.hasNext()) {
                
                Entry<byte[], byte[]> next = indexIt.next();
                String indexKey = new String(next.getKey());
                String indexValue = new String(next.getValue());
                
                Entry<byte[], byte[]> next2 = mapIt.next();
                String mapKey = new String(next2.getKey());
                String mapValue = new String(next2.getValue());
                
                assertEquals(mapKey, indexKey);
                assertEquals(mapValue, indexValue);
            }
        }
        
        {
            // look up a range of elements
            byte[] from = "e".getBytes();
            byte[] to = "f".getBytes();
            
            Iterator<Entry<byte[], byte[]>> mapIt = map.subMap(from, to).entrySet().iterator();
            Iterator<Entry<byte[], byte[]>> indexIt = diskIndex.rangeLookup(from, to, true);
            
            while (indexIt.hasNext() || mapIt.hasNext()) {
                
                Entry<byte[], byte[]> next = indexIt.next();
                String indexKey = new String(next.getKey());
                String indexValue = new String(next.getValue());
                
                Entry<byte[], byte[]> next2 = mapIt.next();
                String mapKey = new String(next2.getKey());
                String mapValue = new String(next2.getValue());
                
                assertEquals(mapKey, indexKey);
                assertEquals(mapValue, indexValue);
            }
        }
        
        {
            // look up another range of elements
            byte[] from = "fd".getBytes();
            byte[] to = "sa".getBytes();
            
            Iterator<Entry<byte[], byte[]>> mapIt = map.subMap(from, to).entrySet().iterator();
            Iterator<Entry<byte[], byte[]>> indexIt = diskIndex.rangeLookup(from, to, true);
            
            while (indexIt.hasNext() || mapIt.hasNext()) {
                
                Entry<byte[], byte[]> next = indexIt.next();
                String indexKey = new String(next.getKey());
                String indexValue = new String(next.getValue());
                
                Entry<byte[], byte[]> next2 = mapIt.next();
                String mapKey = new String(next2.getKey());
                String mapValue = new String(next2.getValue());
                
                assertEquals(mapKey, indexKey);
                assertEquals(mapValue, indexValue);
            }
        }
        
    }
    
    public void estLargeScaleDescendingPrefixLookup() throws Exception {
        
        // initialize a map w/ random strings
        TreeMap<byte[], byte[]> map = new TreeMap<byte[], byte[]>(COMP);
        for (int i = 0; i < NUM_ENTRIES; i++)
            map.put(createRandomString(1, 15).getBytes(), createRandomString(1, 15).getBytes());
        
        // delete old index file
        new File(PATH1).delete();
        
        // write the map to a disk index
        DiskIndexWriter index = new DiskIndexWriter(PATH1, MAX_BLOCK_ENTRIES, COMPRESSED);
        index.writeIndex(map.entrySet().iterator());
        
        // read the disk index
        DiskIndex diskIndex = new DiskIndex(PATH1, new DefaultByteRangeComparator(), COMPRESSED);
        
        {
            // look up the complete list of elements
            Iterator<Entry<byte[], byte[]>> mapIt = map.descendingMap().entrySet().iterator();
            Iterator<Entry<byte[], byte[]>> indexIt = diskIndex.rangeLookup(null, null, false);
            
            while (indexIt.hasNext() || mapIt.hasNext()) {
                
                Entry<byte[], byte[]> next = indexIt.next();
                String indexKey = new String(next.getKey());
                String indexValue = new String(next.getValue());
                
                Entry<byte[], byte[]> next2 = mapIt.next();
                String mapKey = new String(next2.getKey());
                String mapValue = new String(next2.getValue());
                
                assertEquals(mapKey, indexKey);
                assertEquals(mapValue, indexValue);
            }
        }
        
        {
            // look up a range of elements
            byte[] from = "e".getBytes();
            byte[] to = "f".getBytes();
            
            Iterator<Entry<byte[], byte[]>> mapIt = map.descendingMap().subMap(to, from).entrySet().iterator();
            Iterator<Entry<byte[], byte[]>> indexIt = diskIndex.rangeLookup(from, to, false);
            
            while (indexIt.hasNext() || mapIt.hasNext()) {
                
                Entry<byte[], byte[]> next = indexIt.next();
                String indexKey = new String(next.getKey());
                String indexValue = new String(next.getValue());
                
                Entry<byte[], byte[]> next2 = mapIt.next();
                String mapKey = new String(next2.getKey());
                String mapValue = new String(next2.getValue());
                
                assertEquals(mapKey, indexKey);
                assertEquals(mapValue, indexValue);
            }
        }
        
        {
            // look up another range of elements
            byte[] from = "fd".getBytes();
            byte[] to = "sa".getBytes();
            
            Iterator<Entry<byte[], byte[]>> mapIt = map.descendingMap().subMap(to, from).entrySet().iterator();
            Iterator<Entry<byte[], byte[]>> indexIt = diskIndex.rangeLookup(from, to, false);
            
            while (indexIt.hasNext() || mapIt.hasNext()) {
                
                Entry<byte[], byte[]> next = indexIt.next();
                String indexKey = new String(next.getKey());
                String indexValue = new String(next.getValue());
                
                Entry<byte[], byte[]> next2 = mapIt.next();
                String mapKey = new String(next2.getKey());
                String mapValue = new String(next2.getValue());
                
                assertEquals(mapKey, indexKey);
                assertEquals(mapValue, indexValue);
            }
        }
        
    }
    
    private static String createRandomString(int minLength, int maxLength) {
        
        char[] chars = new char[(int) (Math.random() * (maxLength + 1)) + minLength];
        for (int i = 0; i < chars.length; i++)
            chars[i] = (char) (Math.random() * 0xFFFF);
        
        return new String(chars);
    }
    
    private static void populateDiskIndex(byte[][] entries) throws IOException {
        
        SortedMap<byte[], byte[]> testMap = new TreeMap<byte[], byte[]>(COMP);
        for (int i = 0; i < entries.length; i++)
            testMap.put(entries[i], entries[i]);
        
        // write the map to a disk index
        new File(PATH2).delete();
        DiskIndexWriter index = new DiskIndexWriter(PATH2, 2, COMPRESSED);
        index.writeIndex(testMap.entrySet().iterator());
    }
    
    public static void main(String[] args) {
        TestRunner.run(DiskIndexTest.class);
    }
    
}
