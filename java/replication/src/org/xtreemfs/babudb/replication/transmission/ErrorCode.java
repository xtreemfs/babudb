/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission;

import org.xtreemfs.foundation.logging.Logging;

/**
 * Listing of all error codes that might occur on a replication remote request.
 * 
 * @author flangner
 * @since 01/05/2011
 */
public final class ErrorCode {
    public final static int OK = 0;
        
    // congestion ctrl
    public final static int BUSY = 1;
    
    public final static int SERVICE_UNAVAILABLE = 2;
    
    public final static int FILE_UNAVAILABLE = 3;
    
    public final static int LOG_UNAVAILABLE = 4;
    
/*
 * mapping of babuDB specific user errors
 */
    
    public final static int T_DB_EXISTS = 10;
    
    public final static int T_NO_SUCH_DB = 11;
    
    public final static int T_NO_SUCH_INDEX = 12;
    
    public final static int T_SNAP_EXISTS = 13;
    
    public final static int T_NO_SUCH_SNAPSHOT = 14;
    
    public final static int UNKNOWN = 99;
    
    /**
     * Maps BabuDB user errors to an transmittable encoding.
     * 
     * @param e
     * @return the transmission representation of a BabuDB user error, or UNKNOWN, if e was no user 
     *         error.
     */
    public final static int mapUserError(org.xtreemfs.babudb.api.exception.BabuDBException e) {
        
        switch (e.getErrorCode()) {
        case DB_EXISTS :        return ErrorCode.T_DB_EXISTS;
        case NO_SUCH_DB :       return ErrorCode.T_NO_SUCH_DB;
        case NO_SUCH_INDEX :    return ErrorCode.T_NO_SUCH_INDEX;
        case SNAP_EXISTS :      return ErrorCode.T_SNAP_EXISTS;
        case NO_SUCH_SNAPSHOT : return ErrorCode.T_NO_SUCH_SNAPSHOT;
        default :               Logging.logError(Logging.LEVEL_DEBUG, e, e);            
                                return ErrorCode.UNKNOWN;
        }
    }
    
    /**
     * Reverse mapping of the transmitted user error to a BabuDB error code. If no user error was 
     * found it will be decoded to an REPLICATION_FAILURE.
     * 
     * @param e
     * @return the decoded user error, or REPLICATION_FAILURE if no user error fits.
     */
    public final static org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode 
            mapTransmissionError(int e) {
        switch (e) {
        case ErrorCode.T_DB_EXISTS: 
            return org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode.DB_EXISTS;
        case ErrorCode.T_NO_SUCH_DB: 
            return org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode.NO_SUCH_DB;
        case ErrorCode.T_NO_SUCH_INDEX: 
            return org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode.NO_SUCH_INDEX;
        case ErrorCode.T_SNAP_EXISTS: 
            return org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode.SNAP_EXISTS;
        case ErrorCode.T_NO_SUCH_SNAPSHOT: 
            return org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode.NO_SUCH_SNAPSHOT;
        default :               
            return org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode.REPLICATION_FAILURE;
        }
    }
}
