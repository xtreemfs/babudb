/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.overlay.MultiOverlayBufferTree;
import org.xtreemfs.babudb.index.overlay.MultiOverlayStringTree;
import org.xtreemfs.babudb.index.overlay.MultiOverlayTree;
import org.xtreemfs.common.buffer.ReusableBuffer;

public class MultiOverlayTreeTest extends TestCase {
    
    public void testOverlayTree() {
        
        MultiOverlayTree<String, String> tree = new MultiOverlayTree<String, String>("\0");
        tree.insert("3", "bla");
        tree.insert("4", "blub");
        tree.insert("10", "foo");
        int snap1 = tree.newOverlay();
        
        tree.insert("5", "ertz");
        tree.insert("12", "yagga");
        tree.insert("3", null);
        tree.insert("10", "wuerg");
        int snap2 = tree.newOverlay();
        
        tree.insert("7", "bar");
        tree.insert("4", "quark");
        
        assertNull(tree.lookup("3"));
        assertEquals("bla", tree.lookup("3", snap1));
        assertEquals("wuerg", tree.lookup("10"));
        assertEquals("foo", tree.lookup("10", snap1));
        assertEquals("bar", tree.lookup("7"));
        assertNull(tree.lookup("7", snap2));
        assertEquals("blub", tree.lookup("4", snap1));
        assertEquals("blub", tree.lookup("4", snap2));
        assertEquals("quark", tree.lookup("4"));
        
        // randomly insert 200 elements in a map
        final int numElements = 200;
        
        tree = new MultiOverlayTree<String, String>("\0");
        
        final SortedMap<String, String> map1 = new TreeMap<String, String>();
        for (int i = 0x10; i < numElements; i++) {
            String key = Integer.toHexString(i);
            String val = Integer.toHexString((int) (Math.random() * Integer.MAX_VALUE));
            map1.put(key, val);
            tree.insert(key, val);
        }
        
        snap1 = tree.newOverlay();
        final SortedMap<String, String> map2 = new TreeMap<String, String>(map1);
        
        for (int i = 0x10; i < numElements; i += 2) {
            String key = Integer.toHexString(i);
            tree.insert(key, null);
            map2.remove(key);
        }
        
        snap2 = tree.newOverlay();
        final SortedMap<String, String> map3 = new TreeMap<String, String>(map2);
        
        for (int i = 0x10; i < numElements; i += 5) {
            String key = Integer.toHexString(i);
            String val = Integer.toHexString((int) (Math.random() * Integer.MAX_VALUE));
            tree.insert(key, val);
            map3.put(key, val);
        }
        
        Iterator<Entry<String, String>> it = tree.rangeLookup(null, null);
        Iterator<String> itExpected = map3.values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
        
        it = tree.rangeLookup(null, null, snap1);
        itExpected = map1.values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
        
        it = tree.rangeLookup(null, null, snap2);
        itExpected = map2.values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
        
        it = tree.rangeLookup("3", "4");
        itExpected = map3.subMap("3", "4").values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
    }
    
    public void testOverlayStringTree() {
        
        MultiOverlayStringTree<String> tree = new MultiOverlayStringTree<String>("\0");
        
        // randomly insert 200 elements in a map
        final int numElements = 200;
        
        tree = new MultiOverlayStringTree<String>("\0");
        
        final SortedMap<String, String> map1 = new TreeMap<String, String>();
        for (int i = 0x10; i < numElements; i++) {
            String key = Integer.toHexString(i);
            String val = Integer.toHexString((int) (Math.random() * Integer.MAX_VALUE));
            map1.put(key, val);
            tree.insert(key, val);
        }
        
        int snap1 = tree.newOverlay();
        final SortedMap<String, String> map2 = new TreeMap<String, String>(map1);
        
        for (int i = 0x10; i < numElements; i += 2) {
            String key = Integer.toHexString(i);
            tree.insert(key, null);
            map2.remove(key);
        }
        
        int snap2 = tree.newOverlay();
        final SortedMap<String, String> map3 = new TreeMap<String, String>(map2);
        
        for (int i = 0x10; i < numElements; i += 5) {
            String key = Integer.toHexString(i);
            String val = Integer.toHexString((int) (Math.random() * Integer.MAX_VALUE));
            tree.insert(key, val);
            map3.put(key, val);
        }
        
        Iterator<Entry<String, String>> it = tree.prefixLookup(null);
        Iterator<String> itExpected = map3.values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
        
        it = tree.prefixLookup(null, snap1);
        itExpected = map1.values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
        
        it = tree.prefixLookup(null, snap2);
        itExpected = map2.values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
        
        it = tree.prefixLookup("3");
        itExpected = map3.subMap("3", "4").values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
    }
    
    public void testOverlayBufferTree() {
        
        MultiOverlayBufferTree tree = new MultiOverlayBufferTree(new byte[0],
            new DefaultByteRangeComparator());
        
        // randomly insert 200 elements in a map
        final int numElements = 200;
        
        final SortedMap<String, byte[]> map1 = new TreeMap<String, byte[]>();
        for (int i = 0; i < numElements; i++) {
            
            String keyString = Integer.toHexString(i);
            String valString = Integer.toHexString((int) (Math.random() * Integer.MAX_VALUE));
            byte[] key = keyString.getBytes();
            byte[] val = valString.getBytes();
            
            map1.put(keyString, val);
            tree.insert(key, val);
        }
        
        for (int i = 0; i < numElements; i++) {
            
            String keyString = Integer.toHexString(i);
            byte[] key = keyString.getBytes();
            
            assertEquals(tree.lookup(key), map1.get(keyString));
        }
        
        int snap1 = tree.newOverlay();
        final SortedMap<String, byte[]> map2 = new TreeMap<String, byte[]>(map1);
        
        for (int i = 0; i < numElements; i += 2) {
            
            String keyString = Integer.toHexString(i);
            byte[] key = keyString.getBytes();
            
            tree.insert(key, null);
            map2.remove(keyString);
        }
        
        for (int i = 0; i < numElements; i++) {
            
            String keyString = Integer.toHexString(i);
            byte[] key = keyString.getBytes();
            
            assertEquals(tree.lookup(key), map2.get(keyString));
        }
        
        int snap2 = tree.newOverlay();
        final SortedMap<String, byte[]> map3 = new TreeMap<String, byte[]>(map2);
        
        for (int i = 0; i < numElements; i += 5) {
            
            String keyString = Integer.toHexString(i);
            byte[] key = keyString.getBytes();
            byte[] val = Integer.toHexString((int) (Math.random() * Integer.MAX_VALUE)).getBytes();
            
            tree.insert(key, val);
            map3.put(keyString, val);
        }
        
        for (int i = 0; i < numElements; i++) {
            
            String keyString = Integer.toHexString(i);
            byte[] key = keyString.getBytes();
            
            assertEquals(tree.lookup(key), map3.get(keyString));
        }
        
        Iterator<Entry<byte[], byte[]>> it = tree.prefixLookup(null);
        Iterator<byte[]> itExpected = map3.values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
        
        it = tree.prefixLookup(null, snap1);
        itExpected = map1.values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
        
        it = tree.prefixLookup(null, snap2);
        itExpected = map2.values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
        
        it = tree.prefixLookup("3".getBytes());
        itExpected = map3.subMap("3", "4").values().iterator();
        while (it.hasNext())
            assertEquals(itExpected.next(), it.next().getValue());
        assertFalse(itExpected.hasNext());
    }
    
    public static void main(String[] args) {
        TestRunner.run(MultiOverlayTreeTest.class);
    }
    
    protected void assertEquals(ReusableBuffer expected, ReusableBuffer val) {
        
        if (expected == null)
            return;
        
        String e = new String(expected.array());
        String v = new String(val.array());
        
        assertEquals(e, v);
    }
}
