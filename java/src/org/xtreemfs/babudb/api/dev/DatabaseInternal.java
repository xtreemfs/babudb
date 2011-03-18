/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev;

import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;

/**
 * Interface of {@link Database} for internal usage. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * 
 * @author flangner
 * @since 03/18/2011
 */
public interface DatabaseInternal extends Database {

    /**
     * Returns the underlying LSM database implementation.
     * 
     * @return the LSM database
     */
    public LSMDatabase getLSMDB();
}
