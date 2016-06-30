/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.transmission;

import org.xtreemfs.babudb.replication.service.ServiceLayer;
import org.xtreemfs.babudb.replication.transmission.client.ClientFactory;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler;

/**
 * The interface for the {@link ServiceLayer} to access methods of the 
 * {@link TransmissionLayer}.
 *  
 * @author flangner
 * @since 04/14/2010
 */
public interface TransmissionToServiceInterface extends ClientFactory {

    /**
     * @return the {@link FileIOInterface}.
     */
    public FileIOInterface getFileIOInterface();
    
    /**
     * Adds the given request handler to the request dispatcher. Request handler
     * process the request logically depending on the semantic of the interface
     * they represent. Request handler are identified by the ID of such an
     * interface and will replace any already existing one if they have the same
     * ID.
     * 
     * @param requestHandler - identified by its interface id.
     */
    public void addRequestHandler(RequestHandler requestHandler);
}