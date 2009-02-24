/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.include.common.logging.Logging;

import static org.junit.Assert.*;

/**
 *
 * @author bjko
 */
public class LSNTest {

    public LSNTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
        Logging.start(Logging.LEVEL_ERROR);
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of getViewId method, of class LSN.
     */
    @Test
    public void testGetViewId() {
        System.out.println("getViewId");
        LSN instance = new LSN(1,9);
        int expResult = 1;
        int result = instance.getViewId();
        assertEquals(expResult, result);

    }

    /**
     * Test of getSequenceNo method, of class LSN.
     */
    @Test
    public void testGetSequenceNo() {
        System.out.println("getSequenceNo");
        LSN instance = new LSN(1,9);
        long expResult = 9;
        long result = instance.getSequenceNo();
        assertEquals(expResult, result);

    }

    /**
     * Test of compareTo method, of class LSN.
     */
    @Test
    public void testCompareTo() {
        System.out.println("compareTo");
        LSN one = new LSN(1,100);
        LSN two = new LSN(2,100);
        int expResult = -1;
        int result = one.compareTo(two);
        assertEquals(expResult, result);
        
        one = new LSN(1,100);
        two = new LSN(2,100);
        expResult = 1;
        result = two.compareTo(one);
        assertEquals(expResult, result);
        
        one = new LSN(1,100);
        two = new LSN(1,100);
        expResult = 0;
        result = two.compareTo(one);
        assertEquals(expResult, result);
        
        one = new LSN(1,99);
        two = new LSN(1,100);
        expResult = -1;
        result = one.compareTo(two);
        assertEquals(expResult, result);
        
        one = new LSN(1,99);
        two = new LSN(1,100);
        expResult = 1;
        result = two.compareTo(one);
        assertEquals(expResult, result);

    }

}