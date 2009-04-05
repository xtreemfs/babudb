/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequestListener;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.BabuDBImpl;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.ReplicationThread.ReplicationException;
import org.xtreemfs.babudb.replication.RequestPreProcessor.PreProcessException;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.LifeCycleListener;
import org.xtreemfs.include.foundation.pinky.HTTPUtils;
import org.xtreemfs.include.foundation.pinky.PinkyRequest;
import org.xtreemfs.include.foundation.pinky.PinkyRequestListener;
import org.xtreemfs.include.foundation.pinky.SSLOptions;
import org.xtreemfs.include.foundation.speedy.SpeedyRequest;
import org.xtreemfs.include.foundation.speedy.SpeedyResponseListener;

import static org.xtreemfs.babudb.replication.Replication.CONDITION.*;

/**
 * <p>Configurable settings and default approach for the {@link ReplicationThread}.</p>
 * <p>Used to be one single Instance.</p>
 * <p>Thread safe.</p>
 * 
 * @author flangner
 */

public class Replication implements PinkyRequestListener,SpeedyResponseListener,LifeCycleListener,BabuDBRequestListener { 
    
    /**
     * <p>The inner state of the {@link ReplicationThread}.</p>
     * 
     * @author flangner
     */
    public enum CONDITION {
        RUNNING,
        STOPPED,
        LOADING,
        DEAD
    }
    
    /** <p>Default Port, where all {@link ReplicationThread}s designated as master are listening at.</p> */  
    public final static int             MASTER_PORT     = 35666;
    
    /** <p>Default Port, where all {@link ReplicationThread}s designated as slave are listening at.</p> */  
    public final static int             SLAVE_PORT      = 35667;
    
    /** <p>Default chunk size, for initial load of file chunks.</p> */
    public final static long            CHUNK_SIZE      = 5*1024L*1024L;
    
    /** <p>Default maximum number of attempts for every {@link Request}.</p> */
    public final static int             DEFAULT_NO_TRIES= 3;
    
    /** <p>Default maximum number of elements in the pending queue.</p> */
    public final static int             DEFAULT_MAX_Q   = 1000;
    
    /** <p>The {@link InetSocketAddress} of the master, if this BabuDB server is a slave, null otherwise.</p> */
    volatile InetSocketAddress          master          = null;
    
    /** <p>The {@link InetSocketAddress}s of all slaves, if this BabuDB server is a master, null otherwise.</p> */
    volatile List<InetSocketAddress>    slaves          = null;
    
    /** <p>The always alone running {@link ReplicationThread}.</p> */
    private ReplicationThread           replication     = null;
    
    /** <p>Needed for the replication of service functions like create,copy and delete.</p> */
    final BabuDBImpl                    dbInterface;
    
    /** <p>Has to be set on constructor invocation. Works as lock for <code>master</code>, <code>slaves</code> changes.</p> */
    private final AtomicBoolean         isMaster        = new AtomicBoolean();
    
    /** <p>The condition of the replication mechanism.</p> */
    private volatile CONDITION          condition       = STOPPED;
    
    /** <p>Object for synchronization issues.</p> */
    private final Object                lock            = new Object();
    
    /** <p>The port, where the replication listens at.</p> */
    private final int                   port;
    
    /** <p>The options for the SSL-connections established by the replication.</p> */
    private final SSLOptions            SSLOptions;
    
    /** <p>Maximal number of {@link Request} to store in the pending queue.</p> */
    final int                           queueLimit;
    
    /** <p>The user defined maximum count of attempts every {@link Request}. <code>0</code> means infinite number of retries.</p> */
    final int                           maxTries;
    
    /**
     * <p>Actual chosen synchronization mode.</p>
     * <p>syncN == 0: asynchronous mode</br>
     * syncN == slaves.size(): synchronous mode</br>
     * syncN > 0 && syncN < slaves.size(): N -sync mode with N = syncN</p>
     */
    volatile int                        syncN;
    
    /** <p>Holds the identifier of the last written LogEntry.</p>> */
    private final AtomicReference<LSN> lastWrittenLSN  = new AtomicReference<LSN>(new LSN ("1:0"));
    
    /** <p>Needed for the replication mechanism to control context switches on the BabuDB.</p> */
    Lock                                babuDBcontextSwitchLock;
    
    /**
     * <p>Starts the {@link ReplicationThread} with {@link org.xtreemfs.include.foundation.pinky.PipelinedPinky} listening on <code>port</code>.
     * 
     * <p>Is a Master with <code>slaves</code>.</p>
     * 
     * <p>If the <code>sslOption</code> is not <code>null</code> SSL will be used for all connections.</p>
     * 
     * @param port
     * @param slaves
     * @param babu
     * @param mode
     * @param lock
     * @param maxRetries
     * 
     * @throws BabuDBException - if replication could not be initialized.
     */
    public Replication(List<InetSocketAddress> slaves, SSLOptions sslOptions,int port,BabuDBImpl babu, int mode, int qLimit, Lock lock, int maxRetries) throws BabuDBException {
        assert (slaves!=null) : "Replication in master-mode has no slaves attached.";
        assert (babu!=null) : "Replication misses the DB interface.";
        assert (lock!=null) : "Replication misses the DB-switch-lock.";
        
        this.babuDBcontextSwitchLock = lock;
        this.syncN = mode;
        this.slaves = slaves;
        this.isMaster.set(true);
        this.maxTries = maxRetries;
        this.dbInterface = babu;
        this.port = (port==0) ? MASTER_PORT : port;
        this.SSLOptions = sslOptions;
        this.queueLimit = qLimit;
    }
    
    /**
     * <p>Starts the {@link ReplicationThread} with {@link org.xtreemfs.include.foundation.pinky.PipelinedPinky} listening on <code>port</code>.
     * 
     * <p>Is a Slave with a <code>master</code>.</p>
     * 
     * <p>If the <code>sslOption</code> is not <code>null</code> SSL will be used for all connections.</p>
     * 
     * @param port
     * @param master
     * @param sslOptions
     * @param babu
     * @param mode
     * @param qLimit
     * @param maxRetries
     * 
     * @throws BabuDBException - 
     */
    public Replication(InetSocketAddress master, SSLOptions sslOptions, int port, BabuDBImpl babu, int mode, int qLimit, Lock lock, int maxRetries) throws BabuDBException {
        assert (master!=null) : "Replication in slave-mode has no master attached.";
        assert (babu!=null) : "Replication misses the DB interface.";
        assert (lock!=null) : "Replication misses the DB-switch-lock.";

        this.babuDBcontextSwitchLock = lock;
        this.syncN = mode;
        this.master = master;
        this.isMaster.set(false);
        this.maxTries = maxRetries;
        this.dbInterface = babu;
        this.port = (port==0) ? SLAVE_PORT : port;
        this.SSLOptions = sslOptions;
        this.queueLimit = qLimit;
    }

    /**
     * <p>Initializes the replication after all BabuDB-components have been started successfully.</p>
     * @throws BabuDBException if replication could not be initialized.
     */
    public void initialize() throws BabuDBException{
        try {
            replication = new ReplicationThread(this, this.port, this.SSLOptions);
            replication.setLifeCycleListener(this);
            
            synchronized (lock) {
                replication.start();
                
                replication.waitForStartup();
            }
            
            switchCondition(RUNNING);
        } catch (Exception e) {
            throw new BabuDBException(ErrorCode.IO_ERROR,e.getMessage(),e.getCause());
        }
    }

    /**
     * <p>Approach for a Worker to announce a new {@link LogEntry} <code>le</code> to the {@link ReplicationThread}.</p>
     * 
     * @param le
     */
    public void replicateInsert(LogEntry le){
        Logging.logMessage(Logging.LEVEL_TRACE, this, "LogEntry with LSN '"+le.getLSN().toString()+"' is requested to be replicated...");       
        if (isMaster.get()){
            try {
                Request rq = RequestPreProcessor.getReplicationRequest(le,getSlaves());
                if (!replication.enqueueRequest(rq)) throw new Exception("Request is already in the queue: "+rq.toString()); 
            } catch (Exception e) {
                Logging.logMessage(Logging.LEVEL_TRACE, this, "A LogEntry could not be replicated because: "+e.getMessage());
                le.getAttachment().getListener().requestFailed(le.getAttachment().getContext(), new BabuDBException(ErrorCode.REPLICATION_FAILURE,
                        "A LogEntry could not be replicated because: "+e.getMessage()));
            } 
        }
    }

    /**
     * <p>Send a create request to all available slaves.</p>
     * <p>Synchronous function, because if there are any inconsistencies in this case,
     * concerned slaves will produce a lot of errors and will never be consistent again.</p>
     * 
     * @param databaseName
     * @param numIndices
     * @throws BabuDBException - if create request could not be replicated.
     */
    public void create(String databaseName, int numIndices) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_TRACE, this, "Performing request: CREATE ("+databaseName+"|"+numIndices+")");
        if (isMaster.get()){
            try {
                Status<Request> rq = new Status<Request>(RequestPreProcessor.getReplicationRequest(databaseName,numIndices,getSlaves()),replication);
                if (!replication.enqueueRequest(rq)) throw new Exception("Create is already enqueued!");
                if (rq.waitFor()) throw new Exception("Create has failed!");
            } catch (Exception e) {
                dbInterface.replication_runtime_failure(e.getMessage());
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,e.getMessage(),e.getCause());
            }          
        }
    }
    
    /**
     * <p>Send a copy request to all available slaves.</p>
     * <p>Synchronous function, because if there are any inconsistencies in this case,
     * concerned slaves will produce a lot of errors and will never be consistent again.</p>
     * @param sourceDB
     * @param destDB
     * @throws BabuDBException - if create request could not be replicated.
     */
    public void copy(String sourceDB, String destDB) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_TRACE, this, "Performing request: COPY ("+sourceDB+"|"+destDB+")");
        if (isMaster.get()){
            try {
                Status<Request> rq = new Status<Request>(RequestPreProcessor.getReplicationRequest(sourceDB,destDB,getSlaves()),replication);
                if (!replication.enqueueRequest(rq)) throw new Exception("Copy is already enqueued!");
                if (rq.waitFor()) throw new Exception("Copy has failed!");
            } catch (Exception e) {
                dbInterface.replication_runtime_failure(e.getMessage());
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,e.getMessage(),e.getCause());
            }
        }
    }
    
    /**
     * <p>Send a delete request to all available slaves.</p>
     * <p>Synchronous function, because if there are any inconsistencies in this case,
     * concerned slaves will produce a lot of errors and will never be consistent again.</p>
     * @param databaseName
     * @param deleteFiles
     * @throws BabuDBException - if create request could not be replicated.
     */
    public void delete(String databaseName, boolean deleteFiles) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_TRACE, this, "Performing request: DELETE ("+databaseName+"|"+deleteFiles+")");
        if (isMaster.get()){
            try {
                Status<Request> rq = new Status<Request>(RequestPreProcessor.getReplicationRequest(databaseName,deleteFiles,getSlaves()),replication);
                if (!replication.enqueueRequest(rq)) throw new Exception("Delete is already enqueued!");
                if (rq.waitFor()) throw new Exception("Delete has failed!");
            } catch (Exception e) {
                dbInterface.replication_runtime_failure(e.getMessage());
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,e.getMessage(),e.getCause());
            }  
        }
    }  
    
    /**
     * <p>Performs a network broadcast to get the latest LSN from every available DB.</p>
     * 
     * @param babuDBs
     * @return the LSNs of the latest written LogEntries for all given <code>babuDBs</code>
     * @throws BabuDBException if the request could not be performed.
     */
    public Map<InetSocketAddress,LSN> getStates(List<InetSocketAddress> babuDBs) throws BabuDBException{
        Logging.logMessage(Logging.LEVEL_TRACE, this, "Performing request: STATE");
        try{
        	Status<Request> rq = new Status<Request>(RequestPreProcessor.getReplicationRequest(babuDBs),replication);
        	if (!replication.enqueueRequest(rq)) throw new Exception("State is already enqueued!");
        	if (rq.waitFor()) throw new Exception("");
        }catch (Exception e){
        	throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,e.getMessage(),e.getCause());
        }
        
        Map<InetSocketAddress,LSN> result = new HashMap<InetSocketAddress, LSN>();
        for (InetSocketAddress babu : babuDBs)
        	result.put(babu,replication.slavesStatus.get(babu));

        return result;
    }
    
    /**
     * <p>Stops the Replication.</p>
     * (synchronous)
     * @throws BabuDBException if replication could not be stopped. 
     */
    public void shutdown() throws BabuDBException {
        try {
            if (condition.equals(RUNNING)){
                replication.shutdown();
                replication.waitForShutdown();
            }
            switchCondition(DEAD);
        } catch (Exception e) {
            dbInterface.replication_runtime_failure(e.getMessage());
            throw new BabuDBException(ErrorCode.IO_ERROR,"Failed to stop replication.",e.getCause());
        }       
    }
 
/*
 * for the replication package
 */
    
    /**
     * <p>Master Request - Security check.</p>
     * 
     * @param potMaster
     * @return true if <code>potMaster</code> is the designated Master, false otherwise.
     */
    boolean isDesignatedMaster(InetSocketAddress potMaster) {
        if (isMaster.get()) return false;
        
        synchronized (lock) {
            if (master!=null && potMaster!=null)               
                return potMaster.getAddress().getHostAddress().equals(master.getAddress().getHostAddress());                
        }
        
        return false;
    }
    
    /**
     * <p>Slave Request - Security check.</p>
     * 
     * @param potSlave
     * @return true if <code>potSlave</code> is a designated Slave, false otherwise.
     */
    boolean isDesignatedSlave(InetSocketAddress potSlave) {
        if (!isMaster.get()) return false;
        
        synchronized (lock) {
            if (slaves!=null && potSlave!=null){
               // check the message, whether it is from a designated slave, or not
                boolean designated = false;               
                for (InetSocketAddress slave : slaves) {
                    if (potSlave.getAddress().getHostAddress().equals(slave.getAddress().getHostAddress())) {
                        designated = true;
                        break;
                    }
                }
                return designated;
            } 
        }
        
        return false;
    }
    
    /**
     * <p>Sets the lastWrittenLSN to <code>lsn</code>, if it's bigger.</p>
     * @param lsn
     */
    void updateLastWrittenLSN(LSN lsn){
    	boolean running = true;
    	while (running){
	    	LSN actual = lastWrittenLSN.get();
	    	if (actual.compareTo(lsn)<0)
	    		if (!lastWrittenLSN.compareAndSet(actual, lsn)) continue;
	    	
	    	running = false;
    	}
    }
    
/*
 * ResponseListener   
 */
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.pinky.PinkyRequestListener#receiveRequest(org.xtreemfs.foundation.pinky.PinkyRequest)
     */
    @Override
    public void receiveRequest(PinkyRequest theRequest) {
        try {
            Request rq = RequestPreProcessor.getReplicationRequest(theRequest,this);   
           // if request is null, or already in the queue it was already proceeded and will just be responded
            if (rq==null)
	            replication.sendResponse(theRequest);
            else if (!replication.enqueueRequest(rq))
            	throw replication.new ReplicationException("The received request could not be appended to the pending queue.");            	
        }catch (PreProcessException e){
           // if a request could not be parsed for any reason
            Logging.logMessage(Logging.LEVEL_ERROR, this, e.getMessage());
            if (!theRequest.responseSet) theRequest.setResponse(e.status,e.getMessage());
            else assert(false) : "This request must not have already set a response: "+theRequest.toString();
            theRequest.setClose(true); // close the connection, if an error occurred
            replication.sendResponse(theRequest);
        }catch (ReplicationException re){
            // if a request could not be parsed because,or the queue limit was reached
            Logging.logMessage(Logging.LEVEL_ERROR, this, re.getMessage());
            if (!theRequest.responseSet) theRequest.setResponse(HTTPUtils.SC_SERV_UNAVAIL, re.getMessage());
            else assert(false) : "This request must not have already set a response: "+theRequest.toString();
            theRequest.setClose(true); // close the connection, if an error occurred
            replication.sendResponse(theRequest);
        }
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.speedy.SpeedyResponseListener#receiveRequest(org.xtreemfs.foundation.speedy.SpeedyRequest)
     */
    @Override
    public void receiveRequest(SpeedyRequest theRequest) {   
    	Request rq = null;
        try {
            rq = RequestPreProcessor.getReplicationRequest(theRequest,this);
           // if request is null than it was already responded
            if (rq!=null) 
                if (!replication.enqueueRequest(rq)) {
                    Logging.logMessage(Logging.LEVEL_INFO, replication, "Request is already in the queue: "+rq.toString());
                    rq.free();
                }
        }catch (PreProcessException ppe) {
            Logging.logMessage(Logging.LEVEL_WARN, this, ppe.getMessage());
            if (rq!=null) rq.free();
        }catch (ReplicationException re) {
        	Logging.logMessage(Logging.LEVEL_WARN, this, re.getMessage());
        	if (rq!=null) rq.free();
        }catch (Exception e){
        	if (rq!=null) rq.free();
            dbInterface.replication_runtime_failure(e.getMessage());
            Logging.logMessage(Logging.LEVEL_ERROR, this, e.getMessage());
        }   
        theRequest.freeBuffer();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#insertFinished(java.lang.Object)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void insertFinished(Object context) {
        try { 
            if (context==null){
            	String msg = "No informations about the inserted logEntry available!";
            	Logging.logMessage(Logging.LEVEL_ERROR, this, msg);
            	dbInterface.replication_runtime_failure(msg);
            }
            Status<Request> rq = (Status<Request>) context;
            if (rq!=null){
                // save it if it has the last LSN
                updateLastWrittenLSN(rq.getValue().getLSN());
                
                Request parsed = RequestPreProcessor.getReplicationRequest(rq);
                 // send an ACK for the replicated entry
                if (parsed != null) replication.sendACK((Request) parsed);
                
                // XXX change logLevel to LEVEL_ERROR for testing purpose
                Logging.logMessage(Logging.LEVEL_ERROR, replication, "LogEntry inserted successfully: "+rq.getValue().getLSN().toString());
                rq.finished();
            }
        }catch (ReplicationException e) {
            // if the ACK could not be send
            Logging.logMessage(Logging.LEVEL_ERROR, this, "ACK could not be send: "+e.getMessage());
            dbInterface.replication_runtime_failure(e.getMessage());
        }catch (Exception e) {
            // if a request could not be parsed for any reason
            Logging.logMessage(Logging.LEVEL_ERROR, this, e.getMessage());
            dbInterface.replication_runtime_failure(e.getMessage());
        }
    }

    /**
     * Precondition: -request (context) is in the pending-queue of <code>replication</code>
     */
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#requestFailed(java.lang.Object, org.xtreemfs.babudb.BabuDBException)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void requestFailed(Object context, BabuDBException error) {
        try {
             // parse the error and retrieve the informations
            if (context==null) {
            	String msg = "No informations about the not inserted logEntry available!";
            	Logging.logMessage(Logging.LEVEL_ERROR, this, msg);
            	dbInterface.replication_runtime_failure(msg);
            }
            Status<Request> rq = (Status<Request>) context;
            Request loadrq = RequestPreProcessor.getReplicationRequest(error);
            if (!isMaster() && loadrq!=null) {
        		// send LOAD rq
        		replication.enqueueRequest(loadrq);
        		rq.retry();
            } else 
            	rq.failed(error.getMessage(), maxTries);
        }catch (ReplicationException e){
            // if a request could not be handled for any reason
            Logging.logMessage(Logging.LEVEL_ERROR, this, "Received the answer of a malformed request: "+e.getMessage());
            dbInterface.replication_runtime_failure(e.getMessage());
        }
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#lookupFinished(java.lang.Object, byte[])
     */
    @Override
    public void lookupFinished(Object context, byte[] value) {
        throw new UnsupportedOperationException();         
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#prefixLookupFinished(java.lang.Object, java.util.Iterator)
     */
    @Override
    public void prefixLookupFinished(Object context,
            Iterator<Entry<byte[], byte[]>> iterator) {
        throw new UnsupportedOperationException();      
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#userDefinedLookupFinished(java.lang.Object, java.lang.Object)
     */
    @Override
    public void userDefinedLookupFinished(Object context, Object result) {
        throw new UnsupportedOperationException();           
    }

/*
 * LifeCycleListener for the ReplicationThread
 */
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#crashPerformed()
     */
    @Override
    public void crashPerformed() {
        String message = "";
        if (isMaster.get())
            message = "Master ReplicationThread crashed.";
        else
            message = "Slave ReplicationThread crashed.";
        Logging.logMessage(Logging.LEVEL_ERROR, this, message);  
        switchCondition(DEAD);
        dbInterface.replication_runtime_failure(message);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() {
        String message = "";
        if (isMaster.get())
            message = "Master ReplicationThread stopped.";
        else
            message = "Slave ReplicationThread stopped.";       
        switchCondition(DEAD);
        Logging.logMessage(Logging.LEVEL_INFO, this, message);  
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() {
        String message = "";
        if (isMaster.get())
            message = "Master ReplicationThread started.";
        else
            message = "Slave ReplicationThread started.";
        switchCondition(RUNNING);
        Logging.logMessage(Logging.LEVEL_INFO, this, message);  
    } 
    
/*
 * getter/setter   
 */
    
    /**
     * @return the LSN of the last locally written LogEntry
     */
    public LSN getLastWrittenLSN(){
    	return lastWrittenLSN.get();
    }
    
    /**
     * @return the master
     */
    public InetSocketAddress getMaster() {
        synchronized (lock) {
            return master;
        }       
    }

    /**
     * @param master the master to set
     */
    public void setMaster(InetSocketAddress master) throws InterruptedException {
        synchronized (lock) {
            synchronized (replication.halt) {
                replication.halt.set(true);
                replication.halt.wait();
                
                this.master = master;
                this.slaves = null;
                this.isMaster.set(false);
                replication.toSlave();
                
                replication.halt.set(false);
                replication.halt.notify();
            }
        }    
        
        Logging.logMessage(Logging.LEVEL_TRACE, replication, "Replication: Master has been changed. New master: "+master.getHostName());
    }

    /**
     * @return the slaves
     */
    public List<InetSocketAddress> getSlaves() {
        synchronized (lock) {
            return slaves; 
        }     
    }

    /**
     * @param slaves the slaves to set
     * @throws BabuDBException if an error occurs.
     * @throws InterruptedException if the process was interrupted.
     */
    public void setSlaves(List<InetSocketAddress> slaves) throws InterruptedException, BabuDBException {
        int mode = syncN;
        
        if (slaves.size() < mode){
            Logging.logMessage(Logging.LEVEL_WARN, replication, "Replication: mode has been adjusted automatically to fit the new slaves count.");
            mode = slaves.size();
        }
        
        
        synchronized (lock) {
            if (!replication.isAlive()){
                this.slaves = slaves;
                this.syncN = mode;
                this.master = null;
                this.isMaster.set(true);
                replication.toMaster();
            } else {
                synchronized (replication.halt) {
                    replication.halt.set(true);
                    replication.halt.wait();
                    
                    this.slaves = slaves;  
                    this.syncN = mode;
                    this.master = null;
                    this.isMaster.set(true);
                    replication.toMaster();
                    
                    replication.halt.set(false);
                    replication.halt.notify();
                }
            }               
        }
        
        Logging.logMessage(Logging.LEVEL_TRACE, replication, "Replication: slaves have been changed. New: "+slaves.size()+" slaves.");
    }
    
    /**
     * 
     * @return true if this BabuDB works as a master, false if it is a slave.
     */
    public boolean isMaster(){
        return this.isMaster.get();           
    }
    
    /**
     * <p>mode == 0: asynchronous mode</br>
     * mode == slaves.size(): synchronous mode</br>
     * mode > 0 && mode < slaves.size(): N-sync mode with N = mode</p>
     * 
     * @param mode
     * @throws InterruptedException if the process was interrupted.
     * @throws BabuDBException if an error occurs.
     */
    public void setSyncMode(int mode) throws BabuDBException,InterruptedException{  
        if (mode<0 || mode>slaves.size()) throw new BabuDBException(
                ErrorCode.REPLICATION_FAILURE,"The attempt to set an illegal replication-mode ("+mode+") was denied. " +
        	"Legal modes lie between 0 and "+slaves.size());
        
        synchronized (lock) {
            if (!replication.isAlive()){
                this.syncN = mode;
            } else {
                synchronized (replication.halt) {
                    replication.halt.set(true);
                    replication.halt.wait();
                    
                    this.syncN = mode;
                    
                    replication.halt.set(false);
                    replication.halt.notify();
                }
            }
        }
        
        Logging.logMessage(Logging.LEVEL_TRACE, replication, "Replication: SyncMode has been changed. New mode: "+mode+" @: "+slaves.size()+" slaves.");
    }
    
    /**
     * sets the inner state
     * 
     * @param c
     */
    public void switchCondition(CONDITION c){  	
        this.condition = c;
        Logging.logMessage(Logging.LEVEL_TRACE, this, "Condition changed to '"+c.toString()+"'.");
    }
    
    /**
     * 
     * @return the inner state
     */
    public CONDITION getCondition() {
        return this.condition;
    }
    
    /**
     * <p>Sets a new readLock for contextSwitches on babuDB in case of a babuDB reset.</p>
     * 
     * @param l
     */
    public void changeContextSwitchLock(Lock l){
        synchronized (lock) {
            this.babuDBcontextSwitchLock = l;
        }
    }
    
    /**
     * <p>Verify that no other setting of the replication changes while it is paused!</p>
     * 
     * @return the LSN of the last written entry, or null, if the condition is LOADING.Because no consistent state is ensured in this case.
     * @throws InterruptedException
     */
    public LSN pause() throws InterruptedException{
        synchronized (replication.halt) {
            replication.halt.set(true);
            replication.halt.wait();
        }
        if (getCondition()!=LOADING)
        	return getLastWrittenLSN();
        else return null;
    }
    
    /**
     * <p>Verify that no other setting of the replication changes while it is paused!</p>
     * 
     * <p>Resumes the replication processing.</p>
     */
    public void resume(){
        synchronized (replication.halt) {   
            replication.halt.set(false);
            replication.halt.notify();
        }
    }
}
