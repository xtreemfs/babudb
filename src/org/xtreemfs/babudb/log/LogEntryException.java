/*
 * Copyright (c) 2008-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.log;

/**
 *
 * @author bjko
 */
public class LogEntryException extends Exception {

    private static final long serialVersionUID = 2911195875170424834L;

    public LogEntryException(String message) {
        super(message);
    }

}
