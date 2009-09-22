/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.sandbox;

import java.io.File;
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
        options.put("debug", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,Logging.LEVEL_EMERG));
        options.put("compression", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH, false));
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
    	
        final String path = arguments.get(0);
        final Long entriesPerBlock = options.get("blocksize").numValue;
        final int hitrate = options.get("hitrate").numValue.intValue();
        
        final int minStrLen = 1;
        final int maxStrLen = options.get("keylength").numValue.intValue();
        final char minChar = 48;
        final char maxChar = 122;
        
        final int size = Integer.parseInt(arguments.get(1));
        final int lookups = Integer.parseInt(arguments.get(2));
        final Random generator = new Random();
        
        final boolean compress = options.get("compression").switchValue.booleanValue();
        	
        final String input = options.get("input").stringValue.toString();
        
        boolean verbose = false;
        final ArrayList<byte[]> lookupHits = new ArrayList<byte[]>((int) (hitrate*size) + 1);
        
        if (size != 0) {
            // delete old index file
            new File(path).delete();
            
            if(verbose)
            	System.out.println("creating new database with " + size + " random entries ...");
            
            // write the map to a disk index
            DiskIndexWriter index = new DiskIndexWriter(path, entriesPerBlock.intValue(), compress);
            
            if(!input.equals(DEFAULT_DATAGEN)) {
            	index.writeIndex(DataGenerator.fileIterator(lookupHits, input));
            } else {
            	index.writeIndex(DataGenerator.randomIterator(lookupHits, size, hitrate, minStrLen, maxStrLen, minChar, maxChar));
            }
            
        }
        
        // read the disk index
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
        //
        if(verbose)
        	System.out.println("performing " + lookups + " random lookups ...");
        
        // look up each element
        int hits = 0;
        long sumLookups = 0;
        
        Collections.shuffle(lookupHits);
        
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
        
        System.out.print(size + ", ");
        System.out.print(lookups + ", ");
        System.out.print(hits + ", ");
        System.out.print(sumLookups + ", ");
        // lookups/s (lookup throughput)
        System.out.print((int) Math.ceil(((double) lookups / (double) sumLookups) * 1000.0) + ", ");
        System.out.print((int) Math.ceil(((double) iterTime) * 1000.0) + ", ");
        // entries/s (scan throughput)
        System.out.println((int) Math.ceil(((double) size / (double) iterTime) * 1000.0));
        
        diskIndex.destroy();
    }        
}
