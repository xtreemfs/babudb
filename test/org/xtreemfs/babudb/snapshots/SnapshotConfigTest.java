/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.snapshots;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.junit.After;
import org.junit.Before;
import org.xtreemfs.include.common.buffer.ReusableBuffer;

public class SnapshotConfigTest extends TestCase {
    
    @Before
    public void setUp() throws Exception {
    }
    
    @After
    public void tearDown() throws Exception {
    }
    
    public void testSerialize() throws Exception {
        
        SnapshotConfig snap = new SnapshotConfig("test", new int[] { 2, 5, 6 }, new byte[][][] {
            { "bla".getBytes(), "blub".getBytes(), "afd".getBytes() }, { "313131".getBytes() },
            { "xxxxx".getBytes(), "fasd".getBytes() } });
        
        ReusableBuffer buf = snap.serialize(1);
        SnapshotConfig snap2 = SnapshotConfig.deserialize(buf);
        
        assertEquals(snap.getName(), snap2.getName());
        assertEquals(snap.getIndices(), snap2.getIndices());
        for (int i = 0; i < snap.getIndices().length; i++)
            assertEquals(snap.getPrefixes(i), snap2.getPrefixes(i));
        
    }
    
    public void testSerialize2() throws Exception {
        
        SnapshotConfig snap = new SnapshotConfig("yagga", new int[] { 3 }, null);
        
        ReusableBuffer buf = snap.serialize(1);
        SnapshotConfig snap2 = SnapshotConfig.deserialize(buf);
        
        assertEquals(snap.getName(), snap2.getName());
        assertEquals(snap.getIndices(), snap2.getIndices());
        for (int i = 0; i < snap.getIndices().length; i++)
            assertEquals(snap.getPrefixes(i), snap2.getPrefixes(i));
        
    }
    
    public void testSerialize3() throws Exception {
        
        SnapshotConfig snap = new SnapshotConfig("thisisatest", new int[] { 3, 5 }, new byte[][][] {
            { "blub".getBytes() }, null });
        
        ReusableBuffer buf = snap.serialize(1);
        SnapshotConfig snap2 = SnapshotConfig.deserialize(buf);
        
        assertEquals(snap.getName(), snap2.getName());
        assertEquals(snap.getIndices(), snap2.getIndices());
        for (int i = 0; i < snap.getIndices().length; i++)
            assertEquals(snap.getPrefixes(i), snap2.getPrefixes(i));
        
    }
    
    private static void assertEquals(int[] a1, int[] a2) {
        assertEquals(a1.length, a2.length);
        for (int i = 0; i < a1.length; i++)
            assertEquals(a1[i], a2[i]);
    }
    
    private static void assertEquals(byte[] a1, byte[] a2) {
        assertEquals(a1.length, a2.length);
        for (int i = 0; i < a1.length; i++)
            assertEquals(a1[i], a2[i]);
    }
    
    private static void assertEquals(byte[][] b1, byte[][] b2) {
        if (b1 == null)
            assertNull(b2);
        else {
            assertEquals(b1.length, b2.length);
            for (int i = 0; i < b1.length; i++)
                assertEquals(b1[i], b2[i]);
        }
    }
    
    public static void main(String[] args) {
        TestRunner.run(SnapshotConfigTest.class);
    }
    
}
