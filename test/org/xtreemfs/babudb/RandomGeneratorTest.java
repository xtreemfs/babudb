/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.sandbox.RandomGenerator;
import org.xtreemfs.foundation.logging.Logging;

/**
 * 
 * @author flangner
 *
 */

public class RandomGeneratorTest {
	public static int NO_TESTS_PER_CASE = 100;
	
	@Test
	public void testInitialize() {
		Logging.start(Logging.LEVEL_ERROR);

		for (int i=0;i<NO_TESTS_PER_CASE;i++){
			long testSeed = RandomGenerator.getRandomSeed();
			assertEquals(new RandomGenerator().initialize(testSeed).toString(), 
						 new RandomGenerator().initialize(testSeed).toString());
		}
	}
	
	@Test
	public void testLSNInsertGroup() throws Exception {
		// initialization
		Random random = new Random();
		long testSeed = RandomGenerator.getRandomSeed();
		RandomGenerator testGen1 = new RandomGenerator();
		RandomGenerator testGen2 = new RandomGenerator();	
		testGen1.initialize(testSeed);
		testGen2.initialize(testSeed);
		
		assert (RandomGenerator.MAX_SEQUENCENO<((long) Integer.MAX_VALUE)) : "This test cannot handle such a big MAX_SEQUENCENO.";
		int viewID = random.nextInt(RandomGenerator.MAX_VIEWID-(RandomGenerator.MAX_VIEWID/2))+1;
		long sequenceNO = random.nextInt((int) (RandomGenerator.MAX_SEQUENCENO-1L))+1L;
		LSN testLSN = new LSN(viewID,sequenceNO);
		
		for (int i=0;i<NO_TESTS_PER_CASE;i++){
			assertEquals(testGen1.getInsertGroup(testLSN).toString(), testGen2.getInsertGroup(testLSN).toString());
			if (sequenceNO<RandomGenerator.MAX_SEQUENCENO)
				testLSN = new LSN(viewID,++sequenceNO);
			else
				testLSN = new LSN(++viewID,sequenceNO = 1L);
		}
	}
	
	@Test
	public void testLSNLookUpGroup() throws Exception {
		// initialization
		Random random = new Random();
		long testSeed = RandomGenerator.getRandomSeed();
		RandomGenerator testGen1 = new RandomGenerator();
		RandomGenerator testGen2 = new RandomGenerator();	
		testGen1.initialize(testSeed);
		testGen2.initialize(testSeed);
	
		assert (RandomGenerator.MAX_SEQUENCENO<((long) Integer.MAX_VALUE)) : "This test cannot handle such a big MAX_SEQUENCENO.";		
		for (int i=0;i<NO_TESTS_PER_CASE;i++){
			LSN testLSN = new LSN(random.nextInt(RandomGenerator.MAX_VIEWID-1)+1,random.nextInt((int) (RandomGenerator.MAX_SEQUENCENO-1L))+1L);
			assertEquals(testGen1.getLookupGroup(testLSN).toString(), testGen2.getLookupGroup(testLSN).toString());
		}		
	}
	
	@Test
	public void testCrossComparison() throws Exception {
		// initialization
		Random random = new Random();
		long testSeed = RandomGenerator.getRandomSeed();
		RandomGenerator testGen1 = new RandomGenerator();
		RandomGenerator testGen2 = new RandomGenerator();
		testGen1.initialize(testSeed);
		testGen2.initialize(testSeed);
		
		for (int i=0;i<NO_TESTS_PER_CASE;i++){
			LSN testLSN = new LSN(random.nextInt(RandomGenerator.MAX_VIEWID-1)+1,random.nextInt((int) (RandomGenerator.MAX_SEQUENCENO-1L))+1L);
			assertEquals(testGen1.getInsertGroup(testLSN).lookUpCompareable(),testGen2.getLookupGroup(testLSN).toString());
			assertEquals(testGen2.getInsertGroup(testLSN).lookUpCompareable(),testGen1.getLookupGroup(testLSN).toString());
			testGen1.reset();
			testGen2.reset();
		}
	}
}
