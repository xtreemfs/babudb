/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.sandbox;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;
import org.xtreemfs.babudb.sandbox.ContinuesRandomGenerator;
import org.xtreemfs.foundation.logging.Logging;

/**
 * 
 * @author flangner
 *
 */

public class ContinuesRandomGeneratorTest {
	public static int NO_TESTS_PER_CASE = 100;
	public static int MAX_SEQ = 1000;
	
	@Test
	public void testInitialize() {
		Logging.start(Logging.LEVEL_DEBUG);
		Random random = new Random();

		for (int i=0;i<NO_TESTS_PER_CASE;i++){
			long testSeed = ContinuesRandomGenerator.getRandomSeed();
			long seqNo = random.nextInt(MAX_SEQ)+ContinuesRandomGenerator.MIN_SEQUENCENO;
			assertEquals(new ContinuesRandomGenerator(testSeed,seqNo).toString(), 
						 new ContinuesRandomGenerator(testSeed,seqNo).toString());
		}
	}
	
	@Test
	public void testLSNInsertGroup() throws Exception {
		// initialization
		Random random = new Random();
		long testSeed = ContinuesRandomGenerator.getRandomSeed();
		long seqNo = random.nextInt(MAX_SEQ)+ContinuesRandomGenerator.MIN_SEQUENCENO;
        ContinuesRandomGenerator testGen1 = new ContinuesRandomGenerator(testSeed, seqNo);
        ContinuesRandomGenerator testGen2 = new ContinuesRandomGenerator(testSeed, seqNo);
		
		long sequenceNO = random.nextInt((int) (ContinuesRandomGenerator.MAX_SEQUENCENO-1L))+1L;
		
		for (int i=0;i<NO_TESTS_PER_CASE;i++){
			assertEquals(testGen1.getInsertGroup(sequenceNO).toString(), testGen2.getInsertGroup(sequenceNO).toString());
		}
	}
	
	@Test
	public void testLSNLookUpGroup() throws Exception {
		// initialization
		Random random = new Random();
		long testSeed = ContinuesRandomGenerator.getRandomSeed();
		long seqNo = random.nextInt(MAX_SEQ)+ContinuesRandomGenerator.MIN_SEQUENCENO;
        ContinuesRandomGenerator testGen1 = new ContinuesRandomGenerator(testSeed, seqNo);
        ContinuesRandomGenerator testGen2 = new ContinuesRandomGenerator(testSeed, seqNo);
	
		for (int i=0;i<NO_TESTS_PER_CASE;i++){
			long sequenceNO = random.nextInt((int) (ContinuesRandomGenerator.MAX_SEQUENCENO-1L))+1L;
			assertEquals(testGen1.getLookupGroup(sequenceNO).toString(), testGen2.getLookupGroup(sequenceNO).toString());
		}		
	}
	
	@Test
	public void testCrossComparison() throws Exception {
		// initialization
		Random random = new Random();
		long testSeed = ContinuesRandomGenerator.getRandomSeed();
		long seqNo = random.nextInt(MAX_SEQ)+ContinuesRandomGenerator.MIN_SEQUENCENO;
        ContinuesRandomGenerator testGen1 = new ContinuesRandomGenerator(testSeed, seqNo);
        ContinuesRandomGenerator testGen2 = new ContinuesRandomGenerator(testSeed, seqNo);
		
		for (int i=0;i<NO_TESTS_PER_CASE;i++){
			long sequenceNO = random.nextInt((int) (ContinuesRandomGenerator.MAX_SEQUENCENO-1L))+1L;
			assertEquals(testGen1.getInsertGroup(sequenceNO).lookUpCompareable(),testGen2.getLookupGroup(sequenceNO).toString());
			assertEquals(testGen2.getInsertGroup(sequenceNO).lookUpCompareable(),testGen1.getLookupGroup(sequenceNO).toString());
		}
	}
}
