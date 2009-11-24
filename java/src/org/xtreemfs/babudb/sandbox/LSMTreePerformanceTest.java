/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.sandbox;

import java.io.File;
import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.babudb.index.writer.DiskIndexWriter;

public class LSMTreePerformanceTest {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 4) {
            System.out.println("usage: java " + LSMTreePerformanceTest.class.getCanonicalName()
                + " <db_file> <num_entries> <num_inserts> <num_lookups>");
            System.exit(1);
        }
        
        final String path = args[0];
        final int entriesPerBlock = 16;
        
        final int minStrLen = 1;
        final int maxStrLen = 8;
        final char minChar = 48;
        final char maxChar = 122;
        
        final int size = Integer.parseInt(args[1]);
        final int inserts = Integer.parseInt(args[2]);
        final int lookups = Integer.parseInt(args[3]);

        // TODO: make configurable
        final int blockFileSize = 1024*1024*1024;

        if (size != 0) {
            // delete old index file
            new File(path).delete();
            
            System.out.println("creating new database with " + size + " random entries ...");
            
            // write the map to a disk index
            DiskIndexWriter index = new DiskIndexWriter(path, entriesPerBlock, false, blockFileSize);
            index.writeIndex(new Iterator<Entry<byte[], byte[]>>() {
                
                private int    count;
                
                private String next = minChar + "";
                
                @Override
                public boolean hasNext() {
                    return count < size;
                }
                
                @Override
                public Entry<byte[], byte[]> next() {
                    
                    count++;
                    next = createNextString(next, minStrLen, maxStrLen, minChar, maxChar);
                    
                    return new Entry<byte[], byte[]>() {
                        
                        final byte[] nextBytes = next.getBytes();
                        
                        @Override
                        public byte[] getKey() {
                            return nextBytes;
                        }
                        
                        @Override
                        public byte[] getValue() {
                            return nextBytes;
                        }
                        
                        @Override
                        public byte[] setValue(byte[] value) {
                            throw new UnsupportedOperationException();
                        }
                        
                    };
                }
                
                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
                
            });
        }
        
        // read the LSM tree
        LSMTree tree = new LSMTree(path, new DefaultByteRangeComparator(), false);
        
        System.out.println("inserting " + inserts + " random elements ...");
        for (int i = 0; i < inserts; i++) {
            byte[] entry = createRandomString(minChar, maxChar, minStrLen, maxStrLen).getBytes();
            tree.insert(entry, entry);
        }
        
        // Iterator<Entry<ReusableBuffer, ReusableBuffer>> it =
        // tree.prefixLookup(null);
        // while (it.hasNext())
        // System.out.println(new String(it.next().getKey().array()));
        //        
        System.out.println("performing " + lookups + " random lookups ...");
        
        // look up each element
        int hits = 0;
        long t0 = System.currentTimeMillis();
        for (int i = 0; i < lookups; i++) {
            
            byte[] key = createRandomString(minChar, maxChar, minStrLen, maxStrLen).getBytes();
            byte[] result = tree.lookup(key);
            if (result != null)
                hits++;
            
            if (i % 100000 == 0)
                System.out.println(i);
        }
        
        long time = System.currentTimeMillis() - t0;
        
        System.out.println(time + " ms");
        System.out.println((long) lookups * 1000 / time + " lookups/s");
        System.out.println("hits: " + hits + " / " + lookups);
    }
    
    private static String createNextString(String st, int minStrLen, int maxStrLen, char minChar,
        char maxChar) {
        
        char[] chars = st.toCharArray();
        
        for (;;) {
            
            double rnd = Math.random();
            
            if (rnd < .1f + .8f / chars.length && chars.length < maxStrLen) {
                
                // append character
                char[] chars2 = new char[chars.length + 1];
                System.arraycopy(chars, 0, chars2, 0, chars.length);
                chars2[chars2.length - 1] = createRandomChar(minChar, maxChar);
                return new String(chars2);
                
            } else if (rnd > .95f + .05f / chars.length && chars.length > minStrLen) {
                
                int i = chars.length - 2;
                for (; i >= 0; i--)
                    if (chars[i] < maxChar)
                        break;
                
                if (i == -1)
                    continue;
                
                // increment character and truncate
                char[] chars2 = new char[i + 1];
                System.arraycopy(chars, 0, chars2, 0, chars2.length);
                chars2[chars2.length - 1]++;
                return new String(chars2);
                
            } else if (chars[chars.length - 1] < maxChar) {
                
                // increment last character
                chars[chars.length - 1]++;
                return new String(chars);
            }
        }
    }
    
    private static String createRandomString(char minChar, char maxChar, int minLength,
        int maxLength) {
        
        char[] chars = new char[(int) (Math.random() * (maxLength + 1)) + minLength];
        for (int i = 0; i < chars.length; i++)
            chars[i] = createRandomChar(minChar, maxChar);
        
        return new String(chars);
    }
    
    private static char createRandomChar(char minChar, char maxChar) {
        return (char) (Math.random() * (maxChar - minChar + 1) + minChar);
    }
    
}
