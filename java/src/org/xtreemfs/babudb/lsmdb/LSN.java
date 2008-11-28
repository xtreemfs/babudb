/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.lsmdb;

/**
 *
 * @author bjko
 */
public class LSN implements Comparable {

    private final int viewId;
    private final long sequenceNo;
    
    public LSN(int viewId,  long sequenceNo) {
        this.viewId = viewId;
        this.sequenceNo = sequenceNo;
    }

    public int getViewId() {
        return viewId;
    }

    public long getSequenceNo() {
        return sequenceNo;
    }

    public int compareTo(Object o) {
        LSN other = (LSN)o;
        if (this.viewId > other.viewId) {
            return 1;
        } else if (this.viewId < other.viewId) {
            return -1;
        } else {
            if (this.sequenceNo > other.sequenceNo) {
                return 1;
            } else if (this.sequenceNo < other.sequenceNo) {
                return -1;
            } else
                return 0;
        }
    }
    
    
}
