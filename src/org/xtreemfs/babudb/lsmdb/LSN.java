/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.lsmdb;

/**
 *
 * @author bjko
 * @author flangner
 */
public class LSN implements Comparable<LSN> {

    private final int viewId;
    private final long sequenceNo;
    
    public LSN(int viewId,  long sequenceNo) {
        this.viewId = viewId;
        this.sequenceNo = sequenceNo;
    }
    
    public LSN(byte[] lsn) throws NumberFormatException{
        this(new String(lsn));
    }
    
    public LSN(org.xtreemfs.babudb.interfaces.LSN lsn) {
        this.viewId = lsn.getViewId();
        this.sequenceNo = lsn.getSequenceNo();
    }
    
    /**
     * <p>Gets the LSN from given string representation.</p>
     * <p>Pattern: "'viewId':'sequenceNo'"</p>
     * 
     * @param representation
     */
    public LSN(String representation) throws NumberFormatException{
        String[] rep = representation.split(":");
        if (rep.length!=2) throw new NumberFormatException(representation+" is not a legal LSN string-representation.");
    
        viewId = Integer.parseInt(rep[0]);
        sequenceNo = Long.valueOf(rep[1]);
    }

    public int getViewId() {
        return viewId;
    }

    public long getSequenceNo() {
        return sequenceNo;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(LSN o) {
        if (this.viewId > o.viewId) {
            return 1;
        } else if (this.viewId < o.viewId) {
            return -1;
        } else {
            if (this.sequenceNo > o.sequenceNo) {
                return 1;
            } else if (this.sequenceNo < o.sequenceNo) {
                return -1;
            } else
                return 0;
        }
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LSN)
            return ((LSN) obj).viewId == viewId && ((LSN) obj).sequenceNo == sequenceNo;
        else    
            return false;
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return viewId+":"+sequenceNo;
    }
}
