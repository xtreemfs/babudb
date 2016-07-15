/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.replication.control.ControlLayerInterface;

/**
 * Interface for {@link BabuDB} controlling the replication mechanism.
 * 
 * @author flangner
 * @since 04/15/2010
 */
public abstract class TopLayer extends Layer implements ControlLayerInterface {}
