/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.writer;

import java.util.List;

/**
 * A serialized representation of a page.
 * 
 * @author stenjan
 * 
 */
public class SerializedPage {
    
    public SerializedPage(int size, List<Object>... entries) {
        this.entries = entries;
        this.size = size;
    }
    
    public final List<Object>[] entries;
    
    public final int            size;
    
}
