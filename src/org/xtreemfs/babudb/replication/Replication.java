/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequestListener;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.BabuDBImpl;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.ReplicationThread.ReplicationException;
import org.xtreemfs.babudb.replication.RequestPreProcessor.PreProcessException;
import org.xtreemfs.babudb.replication.Status.STATUS;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.LifeCycleListener;
import org.xtreemfs.include.foundation.pinky.HTTPUtils;
import org.xtreemfs.include.foundation.pinky.PinkyRequest;
import org.xtreemfs.include.foundation.pinky.PinkyRequestListener;
import org.xtreemfs.include.foundation.pinky.SSLOptions;
import org.xtreemfs.include.foundation.speedy.SpeedyRequest;
import org.xtreemfs.include.foundation.speedy.SpeedyResponseListener;

/**
 * <p>Configurable settings and default approach for the {@link ReplicationThread}.</p>
 * <p>Used to be one single Instance.</p>
 * <p>Thread safe.</p>
 * 
 * @author flangner
 */

public class Replication implements PinkyRequestListener,SpeedyResponseListener,LifeCycleListener,BabuDBRequestListener {       
    /**
     * <p>Default Port, where all {@link ReplicationThread}s designated as master are listening at.</p> 
     */  
    public final static int             MASTER_PORT     = 35666;
    
    /**
     * <p>Default Port, where all {@link ReplicationThread}s designated as slave are listening at.</p> 
     */  
    public final static int             SLAVE_PORT      = 35667;
    
    /**
     * <p>Default chunk size, for initial load of file chunks.</p>
     */
    public final static long            CHUNK_SIZE      = 5*1024L*1024L;
    
    /**
     * <p>The {@link InetSocketAddress} of the master, if this BabuDB server is a slave, null otherwise.</p>
     */
    volatile InetSocketAddress          master          = null;
    
    /**
     * <p>The {@link InetSocketAddress}s of all slaves, if this BabuDB server is a master, null otherwise.</p>
     */
    volatile List<InetSocketAddress>    slaves          = null;
    
    /**
     * <p>The always alone running {@link ReplicationThread}.</p>
     */
    private ReplicationThread           replication     = null;
    
    /**
     * Needed for the replication of service functions like create,copy and delete.
     */
    final BabuDBImpl                    dbInterface;
    
    /**
     * <p>Has to be set on constructor invocation. Works as lock for <code>master</code>, <code>slaves</code> changes.</p>
     */
    private final AtomicBoolean         isMaster        = new AtomicBoolean();
    
    /**
     * <p>Object for synchronization issues.</p>
     */
    private final Object                lock            = new Object();
    
    /**
     * <p>Actual chosen synchronization modus.</p>
     * <p>syncN == 0: asynchronous mode</br>
     * syncN == slaves.size(): synchronous mode</br>
     * syncN > 0 && syncN < slaves.size(): N -sync mode with N = syncN</p>
     */
    volatile int                        syncN;
    
    /**
     * <p>Holds the identifier of the last written LogEntry.</p>>
     */
    private LSN                         lastWrittenLSN  = new LSN("1:0");
    
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
     * 
     * @throws BabuDBException - if replication could not be initialized.
     */
    public Replication(List<InetSocketAddress> slaves, SSLOptions sslOptions,int port,BabuDBImpl babu, int mode, int qLimit) throws BabuDBException {
        assert (slaves!=null);
        assert (babu!=null);
        
        this.syncN = mode;
        this.slaves = slaves;
        this.isMaster.set(true);
        dbInterface = babu;
        
        initReplication((port==0) ? MASTER_PORT : port, sslOptions, qLimit);
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
     * 
     * @throws BabuDBException - if replication could not be initialized.
     */
    public Replication(InetSocketAddress master, SSLOptions sslOptions, int port, BabuDBImpl babu, int mode, int qLimit) throws BabuDBException {
        assert (master!=null);
        
        this.syncN = mode;
        this.master = master;
        this.isMaster.set(false);
        dbInterface = babu;
        initReplication((port==0) ? SLAVE_PORT : port,sslOptions, qLimit);
    }
    
    private void initReplication(int port,SSLOptions sslOptions, int queueLimit) throws BabuDBException{        
        try {
            replication = new ReplicationThread(this, port, queueLimit, sslOptions);
            replication.setLifeCycleListener(this);
            
            synchronized (lock) {
                replication.start();
                
                replication.waitForStartup();
            }
        } catch (Exception e) {
            throw new BabuDBException(ErrorCode.IO_ERROR,"Failed to startup replication.",e.getCause());
        }
    }


    /**
     * <p>Approach for a Worker to announce a new {@link LogEntry} <code>le</code> to the {@link ReplicationThread}.</p>
     * 
     * @param le
     */
    public void replicateInsert(LogEntry le){
        Logging.logMessage(Logging.LEVEL_TRACE, this, "LogEntry with LSN '"+le.getLSN().toString()+"' is requested to be replicated...");       
        
        try {
            Request rq = RequestPreProcessor.getReplicationRequest(le);
            replication.enqueueRequest(rq); 
        } catch (Exception e) {
            Logging.logMessage(Logging.LEVEL_TRACE, this, "A LogEntry could not be replicated because: "+e.getMessage());
            le.getAttachment().getListener().requestFailed(le.getAttachment().getContext(), new BabuDBException(ErrorCode.REPLICATION_FAILURE,
                    "A LogEntry could not be replicated because: "+e.getMessage()));
        }            
    }
    
    /**
     * <p>Stops the Replication.</p>
     * (synchronous)
     * @throws BabuDBException - if replication could not be stopped. 
     */
    public void stop() throws BabuDBException {
        try {
            replication.shutdown();
            replication.waitForShutdown();
        } catch (Exception e) {
            throw new BabuDBException(ErrorCode.IO_ERROR,"Failed to stop replication.",e.getCause());
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
                Request rq = RequestPreProcessor.getReplicationRequest(databaseName,numIndices);
                replication.sendSynchronousRequest(rq);
            } catch (PreProcessException e) {
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,e.getMessage(),e.getCause());
            } catch (ReplicationException re){
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,re.getMessage(),re.getCause());
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
                Request rq = RequestPreProcessor.getReplicationRequest(sourceDB,destDB);
                replication.sendSynchronousRequest(rq);
            } catch (PreProcessException e) {
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,e.getMessage(),e.getCause());
            } catch (ReplicationException re){
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,re.getMessage(),re.getCause());
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
                Request rq = RequestPreProcessor.getReplicationRequest(databaseName,deleteFiles);
                replication.sendSynchronousRequest(rq);
            } catch (PreProcessException e) {
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,e.getMessage(),e.getCause());
            } catch (ReplicationException re){
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,re.getMessage(),re.getCause());
            }   
        }
    }  
    
    /**
     * <p>Performs a network broadcast.</p>
     * 
     * @param babuDBs
     * @return the LSNs of the latest written LogEntries for all given <code>babuDBs</code>
     */
    public Map<InetSocketAddress,LSN> getStates(List<InetSocketAddress> babuDBs){
        Logging.logMessage(Logging.LEVEL_TRACE, this, "Performing request: STATE");
        return replication.sendStateBroadCast(babuDBs);
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
        synchronized (lock) {
            lastWrittenLSN = (lastWrittenLSN.compareTo(lsn)<0) ? lsn : lastWrittenLSN;
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
           // if rq==null it was already proceeded and will just be responded
            if (rq!=null) replication.enqueueRequest(rq);
            else replication.sendResponse(theRequest);
        }catch (PreProcessException e){
           // if a request could not be parsed for any reason
            Logging.logMessage(Logging.LEVEL_ERROR, this, e.getMessage());
            if (!theRequest.responseSet) theRequest.setResponse(e.status,e.getMessage());
            else assert(false) : "This request must not have already set a response: "+theRequest.toString();
            theRequest.setClose(true); // close the connection, if an error occurred
            replication.sendResponse(theRequest);
        }catch (ReplicationException re){
            // if a request could not be parsed because the queue limit was reached
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
        try {
            Request rq = RequestPreProcessor.getReplicationRequest(theRequest,this);
           // if rq is null than it was already responded
            if (rq!=null) replication.enqueueRequest(rq);
        }catch (Exception e){
            Logging.logMessage(Logging.LEVEL_ERROR, this, e.getMessage());
        }       
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#insertFinished(java.lang.Object)
     */
    @Override
    public void insertFinished(Object context) {
        try { 
            if (context==null) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,"No informations about the inserted logEntry available!");
            Status<Request> rq = (Status<Request>) context;
            if (rq!=null){
                // save it if it has the last LSN
                updateLastWrittenLSN(rq.getValue().getLSN());
                replication.removeMissing(rq.getValue());
                
                Object parsed = RequestPreProcessor.getReplicationRequest(rq);
                 // send an ACK for the replicated entry
                if (parsed instanceof Request) replication.sendACK((Request) parsed);
                else if (parsed instanceof PinkyRequest){
                    replication.sendResponse((PinkyRequest) parsed);
                }                   
            }
        }catch (BabuDBException be) {
            // if a request could not be parsed for any reason
            Logging.logMessage(Logging.LEVEL_ERROR, this, be.getMessage());
        }catch (PreProcessException ppe){       
            Logging.logMessage(Logging.LEVEL_ERROR, this, ppe.getMessage());
        }catch (ReplicationException e) {
            // if the ACK could not be send
            Logging.logMessage(Logging.LEVEL_ERROR, this, "ACK could not be send: "+e.getMessage());
        }
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#requestFailed(java.lang.Object, org.xtreemfs.babudb.BabuDBException)
     */
    @Override
    public void requestFailed(Object context, BabuDBException error) {
        try {
             // parse the error and retrieve the informations
            if (context==null) new Exception("No informations about the inserted logEntry available!");
            Status<Request> erq = (Status<Request>) context;
            Request rq = null;
            if (!isMaster()) rq = RequestPreProcessor.getReplicationRequest(erq,error);
            
             // send the answer
            PinkyRequest orgReq = (PinkyRequest) erq.getValue().getOriginal();
            if (orgReq!=null){
                orgReq.setResponse(HTTPUtils.SC_SERVER_ERROR,"LogEntry could not be replicated due a diskLogger failure: "+error.getMessage());
                replication.sendResponse(orgReq);
            }
            
             // try to deal with the error
            if (rq==null){
                erq.setStatus(STATUS.FAILED);
                replication.enqueueRequest(erq);
            } else {
                replication.enqueueRequest(rq);
                erq.setStatus(STATUS.OPEN);
                replication.enqueueRequest(erq);                  
            }
        }catch (ReplicationException e){
            // if a request could not be handled for any reason
            Logging.logMessage(Logging.LEVEL_ERROR, this, e.getMessage());
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
        Logging.logMessage(Logging.LEVEL_INFO, this, message);  
    } 
    
/*
 * getter/setter   
 */
    
    /**
     * @return the LSN of the last locally written LogEntry
     */
    public LSN getLastWrittenLSN(){
        synchronized (lock){
            return lastWrittenLSN;
        }
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
     */
    public void setSlaves(List<InetSocketAddress> slaves) throws InterruptedException {
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
     * @throws InterruptedException
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
}
