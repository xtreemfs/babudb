/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/
package org.xtreemfs.babudb.replication;

import java.util.LinkedList;
import java.util.List;

/**
 * <p>Holds necessary informations for identifying a Chunk (a piece of a file) explicit.</p>
 * 
 * @author flangner
 */
class Chunk implements Comparable<Chunk> {
    /**
     * name of the file, where the chunk is located at
     */
    private String fileName = null;
    
    /**
     * start inclusive of the byte-range
     */
    private Long begin = 0L;
    /**
     * end inclusive of the byte-range
     */
    private Long end = 0L;
    
    /**
     * <p>Allocates the given range (<code>r1</code>,<code>r2</code>) in the
     * correct order to begin and end of the byte range of file with <code>fName</code>.</p>
     * 
     * @param fName
     * @param r1
     * @param r2
     */
    Chunk(String fName,long r1, long r2) {
        assert(fName!=null);
        
        fileName = fName;
        saveByteRange(r1,r2);
    }
    
    /**
     * <p>Converts the given <code>json</code> back to a {@link Chunk}.</p>
     * 
     * @param json
     */
    Chunk(List<Object> json) {
        fileName = (String) json.get(0);
        assert (fileName!=null);
        saveByteRange((Long) json.get(1), (Long) json.get(2));
    }
    
    /**
     * <p>Allocates the given range (<code>r1</code>,<code>r2</code>) in the
     * correct order to begin and end of the byte range.</p>
     * 
     * @param r1
     * @param r2
     */
    private void saveByteRange(long r1,long r2) {
        if (r1<=r2) {
            begin = r1;
            end = r2;
        }else {
            begin = r2;
            end = r1;
        }
        
        assert (begin!=null);
        assert (end!=null);
    }
    
    /**
     * @return a JSON convertible Map with the chunk informations. 
     */
    List<Object> toJSON(){
        List<Object> result = new LinkedList<Object>();
        result.add(fileName);
        result.add(begin);
        result.add(end);        
        return result;
    }
    
    /**
     * 
     * @return the byte-range beginning position.
     */
    public long getBegin() {
        return begin;
    }
    
    /**
     * 
     * @return the byte-range ending position.
     */
    public long getEnd() {
        return end;
    }
    
    /**
     * 
     * @return the fileName, where the chunk is located at.
     */
    public String getFileName() {
        return fileName;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Chunk o) {
        if (fileName.equals(o.fileName)) {
            if (begin.equals(o.begin)) {
                if (end.equals(o.end)) {
                    return 0;
                }
                return end.compareTo(o.end);
            }
            return begin.compareTo(o.begin);
        }
        return fileName.compareTo(o.fileName);
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        assert(fileName!=null);
        assert(begin!=null);
        assert(end!=null);
        
        Chunk o = (Chunk) obj;
        
        assert(o!=null);
        assert(o.fileName!=null);
        assert(o.begin!=null);
        assert(o.end!=null);
        
        return fileName.equals(o.fileName) && begin.equals(o.begin) && end.equals(o.end);
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Chunk of '"+fileName+"': ["+begin+";"+end+"]";
    }
}
