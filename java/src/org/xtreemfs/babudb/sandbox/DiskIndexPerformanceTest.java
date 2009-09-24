/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.sandbox;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.reader.DiskIndex;
import org.xtreemfs.babudb.index.writer.DiskIndexWriter;
import org.xtreemfs.babudb.sandbox.CLIParser.CliOption;
import org.xtreemfs.include.common.logging.Logging;

public class DiskIndexPerformanceTest {
    private static final String DEFAULT_DATAGEN = "random";
    
    public static void main(String[] args) throws Exception {
   	
        Map<String,CLIParser.CliOption> options = new HashMap<String, CliOption>();
        options.put("path",new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.FILE,new File("/tmp/babudb_benchmark")));
        options.put("blocksize", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,16));
        // hitrate in percent
        options.put("hitrate", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,10));
        options.put("keylength", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,8));
        options.put("scans", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,1000));
        options.put("debug", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,Logging.LEVEL_EMERG));
        options.put("compression", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH, false));
        options.put("overwrite", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH, false));
        options.put("h", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH, false));
        options.put("input", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.STRING, DEFAULT_DATAGEN));
        
        List<String> arguments = new ArrayList(1);
        CLIParser.parseCLI(args, options, arguments);

        if (arguments.size() != 3) {
            System.out.println("usage: java " + DiskIndexPerformanceTest.class.getCanonicalName()
                + " [options] <db_file> <num_entries> <num_lookups>");
            System.exit(1);
        }

    	// start logging when executing this without the entire BabuDB stack
    	Logging.start(options.get("debug").numValue.intValue());
    	
        final Long entriesPerBlock = options.get("blocksize").numValue;
        final int hitrate = options.get("hitrate").numValue.intValue();
        
        final int minStrLen = 1;
        final int maxStrLen = options.get("keylength").numValue.intValue();
        final char minChar = 48;
        final char maxChar = 122;
        
        final int scans = options.get("scans").numValue.intValue();
        	
        int size = Integer.parseInt(arguments.get(1));
        final int lookups = Integer.parseInt(arguments.get(2));
        final Random generator = new Random();
        
        final boolean compress = options.get("compression").switchValue.booleanValue();
        final boolean overwrite = options.get("overwrite").switchValue.booleanValue();
        	
        final String input = options.get("input").stringValue.toString();
        
        if(size == 0 && !input.equals(DEFAULT_DATAGEN)) {
        	/* set the size to number of lines */
        	LineNumberReader lnr = new LineNumberReader(new FileReader(input));
        	while(lnr.readLine() != null) ;
        	size = lnr.getLineNumber();
        }
        
        final String path = arguments.get(0) + "_" + input.substring(input.lastIndexOf("/") + 1) + "_" + "compressed_" + compress + "_" + size + "_" + entriesPerBlock;
        
        boolean verbose = false;
        final ArrayList<byte[]> lookupHits = new ArrayList<byte[]>((int) (hitrate*size) + 1);

        // delete old index file if it should be overwritten
    	if(overwrite)
    		new File(path).delete();

        if (!(new File(path).exists()) || overwrite) {
            if(verbose)
            	System.out.println("creating new database with " + size + " random entries ...");
            
            // write the map to a disk index
            DiskIndexWriter index = new DiskIndexWriter(path, entriesPerBlock.intValue(), compress);
            
            if(!input.equals(DEFAULT_DATAGEN)) {
            	/* note that the iterator must return the items sorted */
            	
            	index.writeIndex(DataGenerator.fileIterator(lookupHits, size, hitrate, input));
            } else {
            	index.writeIndex(DataGenerator.randomIterator(lookupHits, size, hitrate, minStrLen, maxStrLen, minChar, maxChar));
            }
        } else {
        	// 	populate the lookup-hits table
        	DiskIndex diskIndexTmp = new DiskIndex(path, new DefaultByteRangeComparator(), compress);
        	Iterator<Entry<byte[], byte[]>> itTmp = diskIndexTmp.rangeLookup(null, null, true);
        	while(itTmp.hasNext()) {
                if(generator.nextInt() % hitrate == 0)
                	lookupHits.add(itTmp.next().getKey());
        	}
        	diskIndexTmp.destroy();
        }
        
        // do a warm-up phase to trick the JVM
        int warmups = 5;
        
        while(warmups-- > 0) {
        	int readEntries = 10000;        	
        	// 	read the disk index
        	DiskIndex diskIndex = new DiskIndex(path, new DefaultByteRangeComparator(), compress);
        	Iterator<Entry<byte[], byte[]>> it = diskIndex.rangeLookup(null, null, true);
        	while(it.hasNext() && readEntries-- > 0) it.next();
        	diskIndex.destroy();
        }

    	// clear caches...
    	Runtime.getRuntime().exec("/bin/sync");
    	Runtime.getRuntime().exec("/bin/echo 3 > /proc/sys/vm/drop_caches");
    
    	// run garbage collection to remove any existing mmap:ed pages
    	Runtime.getRuntime().gc();
    	
    	// 	read the disk index
    	DiskIndex diskIndex = new DiskIndex(path, new DefaultByteRangeComparator(), compress);
    	Iterator<Entry<byte[], byte[]>> it = diskIndex.rangeLookup(null, null, true);
    
    	/* iterate over all data in the disk index to measure the prefix lookup throughput */
    	long iterStart = System.currentTimeMillis();
    	while(it.hasNext()) it.next();
    	long iterTime = System.currentTimeMillis() - iterStart;

        // Iterator<Entry<ReusableBuffer, ReusableBuffer>> it =
        // diskIndex.rangeLookup(null, null);
        // while (it.hasNext())
        // System.out.println(new String(it.next().getKey().array()));
        
        if(verbose)
        	System.out.println("performing " + lookups + " random lookups ..." + " hits size: " + lookupHits.size());
        
        // look up each element
        int hits = 0;
        long sumLookups = 0;
        
        Collections.shuffle(lookupHits);
        
        /* random lookups, this should put random blocks into memory */
        for (int i = 0; i < lookups; i++) {
            byte[] key;
            
            /* pick a random element that is in the index according to the given hitrate */
        	if(generator.nextInt() % hitrate == 0) {
        		key = lookupHits.get(Math.abs(generator.nextInt()) % lookupHits.size());
        	} else {
        		key = DataGenerator.createRandomString(minChar, maxChar, maxStrLen+1, maxStrLen*2).getBytes();
        	}
        	
            long t0 = System.currentTimeMillis();
            byte[] result = diskIndex.lookup(key);
            sumLookups += System.currentTimeMillis() - t0;
            if (result != null)
                hits++;
            
            //if (i % 100000 == 0 && verbose)
            //    System.out.println(i);
        }
        
        /* random scans */
        long scanTotal = 0;
        for(int i=0; i < scans; i++) {
        	byte[] from;
        	byte[] to;
        	int firstIndex = Math.abs(generator.nextInt()) % lookupHits.size();
        	
        	from = lookupHits.get(firstIndex);
        	to = lookupHits.get(firstIndex + (Math.abs(generator.nextInt()) % (lookupHits.size() - firstIndex)));
        	
            Iterator<Entry<byte[], byte[]>> tmpIt = diskIndex.rangeLookup(from, to, true);
            
            /* iterate over all data returned by the range scan */
            long scanStart = System.currentTimeMillis();
            while(tmpIt.hasNext()) tmpIt.next();
            scanTotal += System.currentTimeMillis() - scanStart;
        }
        
        
        System.out.print(path + ", ");
        System.out.print(size + ", ");
        System.out.print(lookups + ", ");
        System.out.print(hits + ", ");
        System.out.print(sumLookups + ", ");
        /* number of scans */
        System.out.print(scans + ", ");
        /* total time for scans */
        System.out.print(scanTotal + ", ");
        // lookups/s (lookup throughput)
        System.out.print((int) Math.ceil(((double) lookups / (((double) sumLookups) / 1000.0))) + ", ");
        System.out.print((int) Math.ceil((double) iterTime) + ", ");
        // entries/s (scan throughput)
        System.out.println((int) Math.ceil(((double) size / (((double) iterTime) / 1000.0))));
        
        diskIndex.destroy();
    }        
}
