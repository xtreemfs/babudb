/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.index;


public class DefaultByteRangeComparator implements ByteRangeComparator {
    
    @Override
    public byte[][] prefixToRange(byte[] prefix) {
        
        if (prefix == null)
            return new byte[][] { null, null };
        
        byte[] bytes = new byte[prefix.length];
        System.arraycopy(prefix, 0, bytes, 0, prefix.length);
        
        for (int i = bytes.length - 1; i >= 0; i--) {
            if (i == 0 && bytes[i] == Byte.MAX_VALUE)
                return null;
            bytes[i]++;
            if (bytes[i] != Byte.MIN_VALUE)
                break;
        }
        
        return new byte[][] { prefix, bytes };
    }
    
    @Override
    public int compare(ByteRange rng, byte[] buf) {
        
        int n = rng.getStartOffset() + Math.min(rng.getSize(), buf.length);
        int j = 0;
        for (int i = rng.getStartOffset(); i < n; i++, j++) {
            byte v1 = rng.getBuf().get(i);
            byte v2 = buf[j];
            if (v1 == v2)
                continue;
            if (v1 < v2)
                return -1;
            return 1;
        }
        
        return rng.getSize() - buf.length;
    }
    
    @Override
    public int compare(byte[] buf1, byte[] buf2) {
        
        int n = Math.min(buf1.length, buf2.length);
        for (int i = 0, j = 0; i < n; i++, j++) {
            byte v1 = buf1[i];
            byte v2 = buf2[j];
            if (v1 == v2)
                continue;
            if (v1 < v2)
                return -1;
            return 1;
        }
        
        return buf1.length - buf2.length;
    }
}
