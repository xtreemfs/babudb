/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.include.common.util;

/**
 * 
 * @author bjko
 */
public final class OutputUtils {
    
    public static final char[] trHex = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A',
        'B', 'C', 'D', 'E', 'F'     };
    
    public static final String byteToHexString(byte b) {
        StringBuilder sb = new StringBuilder(2);
        sb.append(trHex[((b >> 4) & 0x0F)]);
        sb.append(trHex[(b & 0x0F)]);
        return sb.toString();
    }
    
    public static final String byteArrayToHexString(byte[] array) {
        StringBuilder sb = new StringBuilder(2 * array.length);
        for (byte b : array) {
            sb.append(trHex[((b >> 4) & 0x0F)]);
            sb.append(trHex[(b & 0x0F)]);
        }
        return sb.toString();
    }
    
    public static final String byteArrayToFormattedHexString(byte[] array) {
        StringBuilder sb = new StringBuilder(2 * array.length);
        for (int i = 0; i < array.length; i++) {
            sb.append(trHex[((array[i] >> 4) & 0x0F)]);
            sb.append(trHex[(array[i] & 0x0F)]);
            if (i % 4 == 3) {
                if (i % 16 == 15)
                    sb.append("\n");
                else
                    sb.append(" ");
            }
            
        }
        return sb.toString();
    }
    
    public static String formatBytes(long bytes) {
        
        double kb = bytes / 1024.0;
        double mb = bytes / (1024.0 * 1024.0);
        double gb = bytes / (1024.0 * 1024.0 * 1024.0);
        double tb = bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
        
        if (tb >= 1.0) {
            return String.format("%.2f TB", tb);
        } else if (gb >= 1.0) {
            return String.format("%.2f GB", gb);
        } else if (mb >= 1.0) {
            return String.format("%.2f MB", mb);
        } else if (kb >= 1.0) {
            return String.format("%.2f kB", kb);
        } else {
            return bytes + " bytes";
        }
    }
    
}
