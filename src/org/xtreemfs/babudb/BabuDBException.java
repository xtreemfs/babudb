/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb;

/**
 * A BabuDB database exception class.
 * @author bjko
 */
public class BabuDBException extends Exception {  
    /** */
    private static final long serialVersionUID = -5430755494607838206L;

    public enum ErrorCode {
        /**
         * The requested database does not exist
         */
        NO_SUCH_DB,
        /**
         * A database with this name already exists
         */
        DB_EXISTS,
        /**
         * The requested index does not exist in the database
         */
        NO_SUCH_INDEX,
        /**
         * Data could not be written/read.
         */
        IO_ERROR,
        
        /**
         * If the replication fails.
         */
        REPLICATION_FAILURE,
        
        /**
         * Everything else that went wrong
         */
        INTERNAL_ERROR
    };
    
    private final ErrorCode error;
    
    /**
     * Creates a new BabuDBException
     * @param code the error code
     * @param message an error message
1     */
    public BabuDBException(ErrorCode code, String message) {
        super(message);
        error = code;
    }
    
    /**
     * Creates a new BabuDBException
     * @param code the error code
     * @param message an error message
1     */
    public BabuDBException(ErrorCode code, String message, Throwable rootcause) {
        super(message,rootcause);
        error = code;
    }
 
    /**
     * Get the error message.
     * @return a string representation
     */
    public String getMessage() {
        return super.getMessage()+" (error code: "+error+")";
    }
    
    /**
     * Get the error code.
     * @return error code of the exception.
     */
    public ErrorCode getErrorCode() {
        return error;
    }

}
