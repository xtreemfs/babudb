/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package org.xtreemfs.babudb.sandbox;

/**
 * 
 * @author flangner
 *
 */

public class BabuDBLongrunTestConfig {
    public static final boolean CLUSTER = false;
	
    // the interval to sleep, if consistency-check has occurred before
    public final static int CCHECK_SLEEP_INTERVAL;
	
    // the interval to sleep, if any other event occurred before
    public final static int MIN_SLEEP_INTERVAL;
    public final static int MAX_SLEEP_INTERVAL;
		
    public final static int MAX_DOWN_TIME;
	
    public final static String PATH;
	
    public final static long MIN_SEQUENCENO;
    
    public final static long MAX_SEQUENCENO;
    
    static {
    	if (CLUSTER){
    	    CCHECK_SLEEP_INTERVAL = 3*60*1000;
    	    MIN_SLEEP_INTERVAL = 20*60*1000;
    	    MAX_SLEEP_INTERVAL = 30*60*1000;
    	    MAX_DOWN_TIME = 10*60*1000;
    	    PATH = "/scratch/babuDB/data/";
    	    MIN_SEQUENCENO = 1000L;
    	    MAX_SEQUENCENO = Integer.MAX_VALUE;
    	}else{
    	    CCHECK_SLEEP_INTERVAL = 30*1000;
    	    MIN_SLEEP_INTERVAL = 3*60*1000;
    	    MAX_SLEEP_INTERVAL = 5*60*1000;
    	    MAX_DOWN_TIME = 2*60*1000;
    	    PATH = "/tmp/babuDB/";
    	    MIN_SEQUENCENO = 100L;
    	    MAX_SEQUENCENO = 10000L;
    	}
    }
}
