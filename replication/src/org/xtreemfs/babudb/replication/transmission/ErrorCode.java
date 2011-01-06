/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission;

/**
 * Listing of all error codes that might occur on a replication remote request.
 * 
 * @author flangner
 * @since 01/05/2011
 */
public final class ErrorCode {
    public final static int OK = 0;
        
    public final static int BUSY = 1;
    
    public final static int FILE_UNAVAILABLE = 2;
    
    public final static int LOG_UNAVAILABLE = 3;
    
    public final static int SERVICE_UNAVAILABLE = 4;
    
    public final static int UNKNOWN = 99;
}
