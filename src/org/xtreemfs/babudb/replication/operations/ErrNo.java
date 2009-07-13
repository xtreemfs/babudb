/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

/**
 * Error message IDs.
 * 
 * @since 05/02/2009
 * @author flangner
 */

public enum ErrNo {
    SLAVE_OUT_OF_SYNC,
    SERVICE_CALL_MISSED,
    SECURITY,
    LOG_CUT,
    FILE_UNAVAILABLE,
    INTERNAL_ERROR,
    TOO_BUSY,
}