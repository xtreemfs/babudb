/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;

import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.LifeCycleListener;
import org.xtreemfs.include.foundation.pinky.HTTPUtils;
import org.xtreemfs.include.foundation.pinky.PinkyRequest;
import org.xtreemfs.include.foundation.pinky.PipelinedPinky;
import org.xtreemfs.include.foundation.pinky.SSLOptions;
import org.xtreemfs.include.foundation.pinky.HTTPUtils.DATA_TYPE;
import org.xtreemfs.include.foundation.speedy.MultiSpeedy;
import org.xtreemfs.include.foundation.speedy.SpeedyRequest;

/**
 * <p>Controls the connection between the different BabuDBSystems used by the
 * Replication.</p>
 * 
 * <p>Actually realized with {@link PipelinedPinky} and {@link MultiSpeedy}.</p>
 * @author flangner
 */

class InterBabuConnection implements UncaughtExceptionHandler, LifeCycleListener{
	
    /**
     * <p>Exception for errors while establishing a connection between two BabuDBs.</p>
     * 
     * @author flangner
     *
     */
    class BabuDBConnectionException extends Exception {
        /***/
        private static final long serialVersionUID = -3588307362070344055L;

        /**
         * <p>The reason for this exception is here: <code>msg</code>.</p>
         * 
         * @param msg
         */
        BabuDBConnectionException(String msg) {
            super(msg);
        }
    }
	
    private final int port;
    private final SSLOptions ssl;
    private final Replication frontEnd;
    
    /** <p>Pinky - Thread.</p> */
    private PipelinedPinky pinky;
    
    /** <p> {@link MultiSpeedy} - Thread.</p> */
    private MultiSpeedy speedy;
    
    /**
     * <p>Saves the configuration.</p>
     * 
     * @param port
     * @param sslOptions
     * @param frontEnd
     * @throws BabuDBConnectionException if startup was not successful.
     */
    InterBabuConnection(int port, SSLOptions sslOptions, Replication frontEnd) {
        this.port = port;
        this.ssl = sslOptions;
        this.frontEnd = frontEnd;
    }
    
    /**
     * <p>Starts pinky and speedy.</p>
     * 
     * @throws BabuDBConnectionException
     */
    void start() throws BabuDBConnectionException{
        try{
            // setup pinky
             pinky = new PipelinedPinky(port,null,frontEnd,ssl);
             pinky.setLifeCycleListener(this);
             pinky.setUncaughtExceptionHandler(this);
             
            // setup speedy
             speedy = new MultiSpeedy(ssl);
             speedy.registerSingleListener(frontEnd);
             
             speedy.setLifeCycleListener(this);
             speedy.setUncaughtExceptionHandler(this);
             
             // start pinky
             pinky.start();
             pinky.waitForStartup();
             
            // start speedy
             speedy.start();
             speedy.waitForStartup(); 
         }catch (Exception e){
             String msg = "Could not initialize the inter-BabuDB-connection-components because: "+e.getMessage();
             Logging.logMessage(Logging.LEVEL_ERROR, this, msg);
             throw new BabuDBConnectionException(msg);
         }
    }
    
    /**
     * <p>Stops pinky and speedy.</p>
     * @throws Exception if an error occurs.
     */
    void shutdown() throws Exception{
        if (pinky!=null && pinky.isAlive()){
            pinky.shutdown();
            pinky.waitForShutdown();
        }
        if (speedy!=null && speedy.isAlive()){
            speedy.shutdown();
            speedy.waitForShutdown();
        }
    }
    
    /**
     * <p>Checks the {@link PinkyRequest} <code>rq</code> against <code>null</code>, <br>
     * before sending a Response. Sets a response if it was not set jet.</p>
     *  
     * @param rq
     */
    void sendResponse(PinkyRequest rq) {
        if (rq!=null) {
            if (!rq.responseSet)
                rq.setResponse(HTTPUtils.SC_OKAY); 
            
            pinky.sendResponse(rq);                
        }      
    }
 
    /**
     * <p>Builds up a {@link SpeedyRequest} and sends it to the given destination.</p>
     * 
     * @param token
     * @param attachment
     * @param destination
     * @throws BabuDBConnectionException if request could not be send.
     */
    void sendRequest(Token token, Object attachment, InetSocketAddress destination) throws BabuDBConnectionException {
    	sendRequest(token,(ReusableBuffer) null,attachment,destination);
    }
    
    /**
     * <p>Builds up a {@link SpeedyRequest} and sends it to the given destination.</p>
     * 
     * @param token
     * @param value
     * @param attachment
     * @param destination
     * @throws BabuDBConnectionException if request could not be send.
     */
    void sendRequest(Token token, byte[] value, Object attachment, InetSocketAddress destination) throws BabuDBConnectionException {
    	ReusableBuffer buf = ReusableBuffer.wrap(value);
	sendRequest(token, buf,attachment,destination);
	BufferPool.free(buf);
	buf = null;
    }
    
    /**
     * <p>Builds up a {@link SpeedyRequest} and sends it to the given destination.</p>
     * 
     * @param token
     * @param buffer - creates a viewBuffer on the given.
     * @param attachment
     * @param destination
     * @throws BabuDBConnectionException if request could not be send.
     */
    void sendRequest(Token token, ReusableBuffer buffer, Object attachment, InetSocketAddress destination) throws BabuDBConnectionException{  
	SpeedyRequest sReq = null;    
	try{
	    sReq = new SpeedyRequest(HTTPUtils.POST_TOKEN,
		token.toString(),null,null,buffer.createViewBuffer(),DATA_TYPE.BINARY);
	    sReq.genericAttatchment = attachment;
		    
	    speedy.sendRequest(sReq, destination);  
	} catch (Exception e) {
	    if (sReq != null) sReq.freeBuffer();
	    throw new BabuDBConnectionException("'"+token.toString()+"' could not be send to '"+destination.toString()+"' because: "+e.getMessage());
	}    
	Logging.logMessage(Logging.LEVEL_TRACE, this, "'"+token.toString()+"' was send to '"+destination.toString()+"'");
    }
    
/*
 * LifeCycleListener for Pinky & Speedy
 */
        
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#crashPerformed()
     */  
    @Override
    public void crashPerformed() {
        Logging.logMessage(Logging.LEVEL_ERROR, this, "A InterBabuDBConnection component crashed.");       

        if (pinky!=null && pinky.isAlive()) pinky.shutdown();
        if (speedy!=null && speedy.isAlive()) speedy.shutdown();  
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() {
        Logging.logMessage(Logging.LEVEL_INFO, this, "InterBabuDBConnection component stopped.");      
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() {
        Logging.logMessage(Logging.LEVEL_INFO, this, "InterBabuDBConnection component started.");  
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        Logging.logMessage(Logging.LEVEL_ERROR, this, "Critical error in thread: "+t.getName()+" because: "+e.getMessage());
    } 
}
