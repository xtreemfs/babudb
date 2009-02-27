/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/
package org.xtreemfs.babudb.replication;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogFile;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSN;
// import org.xtreemfs.babudb.replication.Replication.SYNC_MODUS;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.LifeCycleListener;
import org.xtreemfs.include.foundation.LifeCycleThread;
import org.xtreemfs.include.foundation.json.JSONParser;
import org.xtreemfs.include.foundation.pinky.HTTPUtils;
import org.xtreemfs.include.foundation.pinky.PinkyRequest;
import org.xtreemfs.include.foundation.pinky.PipelinedPinky;
import org.xtreemfs.include.foundation.pinky.SSLOptions;
import org.xtreemfs.include.foundation.pinky.HTTPUtils.DATA_TYPE;
import org.xtreemfs.include.foundation.speedy.MultiSpeedy;
import org.xtreemfs.include.foundation.speedy.SpeedyRequest;

import static org.xtreemfs.babudb.replication.Status.STATUS.*;

/**
 * <p>Master-Slave-replication</p>
 * 
 * <p>The Stage for sending DB-changes from the Master to the Servers, and
 * receiving such changes from the Master.</p>
 * 
 * <p>Thread-safe.</p>
 * 
 * <p>Handles a {@link PipelinedPinky}, and a {@link MultiSpeedy} for bidirectional
 * communication with slaves, if the DB is configured as master, or with the master,
 * if it is configured as slave.</p>
 * 
 * @author flangner
 *
 */
class ReplicationThread extends LifeCycleThread implements LifeCycleListener,UncaughtExceptionHandler{
    
    /**
     * <p>Exception occurs while the replication process.</p>
     * 
     * @author flangner
     *
     */
    class ReplicationException extends Exception {
        /***/
        private static final long serialVersionUID = -3588307362070344055L;

        /**
         * <p>The reason for this exception is here: <code>msg</code>.</p>
         * 
         * @param msg
         */
        ReplicationException(String msg) {
            super(msg);
        }
    }
    
    /**
     * <p>Milliseconds idle-time, if the pending and the missing queues are empty.</p>
     */
    final static int TIMEOUT_GRANULARITY = 250;
    
    /**
     * <p>Requests that are waiting to be done.</p>
     */
    private Queue<Status<Request>> pending;
    
    /**
     * <p>LSN's of missing {@link LogEntry}s.</p>
     */
    private Queue<Status<LSN>> missing;
    
    /**
     * <p>Details for missing file-chunks. File Name + [chunk-beginL,chunk-endL].</p>
     */
    private Queue<Status<Chunk>> missingChunks;
    
    /**
     * <p>Table of slaves with their latest acknowledged {@link LSN}s.</p>
     */
    private SlavesStatus slavesStatus = null;
    
    /**
     * <p>Pinky - Thread.</p>
     */
    private PipelinedPinky pinky = null;
    
    /**
     * <p> {@link MultiSpeedy} - Thread.</p>
     */
    private MultiSpeedy speedy = null;
    
    /**
     * <p>Holds all variable configuration parameters and is the approach for the complete replication.</p>
     */
    private Replication frontEnd = null;
    
    /**
     * <p>Flag which implies, that this {@link LifeCycleThread} is running.</p>
     */
    private boolean running = false;

    /**
     * <p>Flag which implies the Thread to halt and notify the waiting application.</p>
     */
    AtomicBoolean halt = new AtomicBoolean(false);
    
    /**
     * <p>Lazy holder for the diskLogFile.</p>
     */
    private DiskLogFile diskLogFile = null;
    
    /**
     * <p>A request dummy with the lowest priority for the pending queue.</p>
     */
    private static final Status<Request> LOWEST_PRIORITY_REQUEST = new Status<Request>(null,FAILED);
    
    /**
     * <p>If a request could not be processed it will be noticed to wait for newer entries for example.
     * With that polling on the same Request is avoided.</p> 
     * <p>If the pendingQueueLimit is reached, requests with a lower priority than the nextExpected will be aborted.</p>
     */
    private Status<Request> nextExpected = LOWEST_PRIORITY_REQUEST;
    
    /**
     * <p>Maximal number of {@link Request} to store in the pending queue.</p> 
     */
    private final int pendingQueueLimit;
    
    /**
     * <p>Default constructor. Synchronous startup for pinky and speedy component.</p>
     * 
     * @param frontEnd 
     * @param port 
     * @param pendingLimit
     * @param sslOptions
     */
    ReplicationThread(Replication frontEnd, int port, int pendingLimit, SSLOptions sslOptions) throws ReplicationException{
        super((frontEnd.isMaster()) ? "Master" : "Slave");
        this.pendingQueueLimit = pendingLimit;       
        this.frontEnd = frontEnd;
        pending = new PriorityBlockingQueue<Status<Request>>();
        missing = (!frontEnd.isMaster()) ? new PriorityBlockingQueue<Status<LSN>>() : null;
        missingChunks = (!frontEnd.isMaster()) ? new PriorityBlockingQueue<Status<Chunk>>() : null;
        slavesStatus = (frontEnd.isMaster()) ? new SlavesStatus(frontEnd.slaves) : null;

        try{
           // setup pinky
            pinky = new PipelinedPinky(port,null,frontEnd,sslOptions);
            pinky.setLifeCycleListener(this);
            pinky.setUncaughtExceptionHandler(this);
            
           // setup speedy
            speedy = new MultiSpeedy(sslOptions);
            speedy.registerSingleListener(frontEnd);
            
            speedy.setLifeCycleListener(this);
            speedy.setUncaughtExceptionHandler(this);
        }catch (IOException io){
            String msg = "Could not initialize the Replication because: "+io.getMessage();
            Logging.logMessage(Logging.LEVEL_ERROR, this, msg);
            throw new ReplicationException(msg);
        }

    }
    
/*
 * LifeCycleThread    
 */
    
    /*
     * (non-Javadoc)
     * @see java.lang.Thread#start()
     */
    @Override
    public synchronized void start() {
        try {
           // start pinky
            pinky.start();
            pinky.waitForStartup();
            
           // start speedy
            speedy.start();
            speedy.waitForStartup();  
            
           // start the lifeCycle
            running = true;
            super.start();
        }catch (Exception io){
            String msg = "ReplicationThread crashed, because: "+io.getMessage();
            Logging.logMessage(Logging.LEVEL_ERROR, this, msg);
            notifyCrashed(new ReplicationException(msg)); 
        }  
    }
    
    /**
     * <p>Work the requestQueue <code>pending</code>.</p>
     */
    @Override
    public void run() {  
        final Checksum checkSum = new CRC32(); 
   
        super.notifyStarted();
        while (running) {
          //initializing...
            Chunk chunk = null;
            LSMDBRequest context = null;
            InetSocketAddress source = null;
            PinkyRequest orgReq = null;
            ReusableBuffer buffer = null;
            ReusableBuffer data = null;
            LSN lsn = null;
            Map<String, List<Long>> metaDataLSM = null;
            
          // check for missing LSNs and missing file chunks -highest priority- (just for slaves)          
            if (!frontEnd.isMaster()) {
                try{
                   // if there are any missing file chunks waiting for them has to be synchronous!
                    Status<Chunk> missingChunk = missingChunks.peek();
                    if (missingChunk != null && missingChunk.getStatus().compareTo(PENDING)<0) 
                        if (missingChunks.remove(missingChunk))
                            sendCHUNK(missingChunk);               
                    
                    Status<LSN> missingLSN = missing.peek();
                    if (missingLSN != null && missingLSN.getStatus().compareTo(PENDING)<0)
                        sendRQ(missingLSN);
                }catch (ReplicationException re){
                    Logging.logMessage(Logging.LEVEL_ERROR, this, "The master seems to be not available for information-retrieval: "+re.getLocalizedMessage());
                }
            }  
          // get a new request 
            Status<Request> newRequest = pending.peek();     
            if (newRequest != null && hasHighPriority(newRequest) && !newRequest.getStatus().equals(PENDING)) { 
                pending.remove(newRequest);
                resetNextExpected();
                
                try{
                    chunk = newRequest.getValue().getChunkDetails();
                    source = newRequest.getValue().getSource();
                    orgReq = newRequest.getValue().getOriginal();
                    context = newRequest.getValue().getContext();
                    data = newRequest.getValue().getData();
                    lsn = newRequest.getValue().getLSN();
                    metaDataLSM = newRequest.getValue().getLsmDbMetaData();
                    
                    switch(newRequest.getValue().getToken()) {
                    
                  // master's logic:
                    
                     // sends logEntry to the slaves
                    case REPLICA_BROADCAST :   
                        int replicasFailed = 0;
                        synchronized (frontEnd.slaves) {
                            final int slaveCount = frontEnd.slaves.size();
                            newRequest.getValue().setMaxReceivableACKs(slaveCount);
                            newRequest.getValue().setMinExpectableACKs(frontEnd.syncN);
                            
                            for (InetSocketAddress slave : frontEnd.slaves) {
                                // build the request
                                SpeedyRequest rq = new SpeedyRequest(HTTPUtils.POST_TOKEN,
                                                                     Token.REPLICA.toString(),null,null,
                                                                     data.createViewBuffer(),
                                                                     DATA_TYPE.BINARY);
                                rq.genericAttatchment = newRequest;
                                
                                // send the request
                                try{  
                                    speedy.sendRequest(rq, slave); 
                                } catch (Exception e){
                                    rq.freeBuffer();
                                    replicasFailed++;
                                    Logging.logMessage(Logging.LEVEL_ERROR, this, "REPLICA could not be send to slave: '"+slave.toString()+"', because: "+e.getMessage());
                                }
                            }
                                      
                            if (replicasFailed > 0 && !newRequest.getValue().decreaseMaxReceivableACKs(replicasFailed))
                                throw new ReplicationException("The replication was not fully successful. '"+replicasFailed+"' of '"+slaveCount+"' slaves could not be reached.");
                            
                             // ASYNC mode unavailable slaves will be ignored for the response   
                            if(frontEnd.syncN == 0){ 
                                context.getListener().insertFinished(context);
                                newRequest.getValue().free();
                            } else {                                                          
                                newRequest.setStatus(PENDING);
                                pending.add(newRequest);
                            }
                        }
                        break;
    
                     // answers a slaves logEntry request
                    case RQ : 
                        boolean notFound = true;
                        LSN last;
                        LogEntry le = null;
                        String latestFileName;
                      
                       // get the latest logFile
                        try {
                            latestFileName = frontEnd.dbInterface.logger.getLatestLogFileName();
                            diskLogFile = new DiskLogFile(latestFileName);                           
                            last = frontEnd.dbInterface.logger.getLatestLSN();
                        } catch (IOException e) {
                            // make one attempt to retry (logFile could have been switched)
                            try {
                                latestFileName = frontEnd.dbInterface.logger.getLatestLogFileName();
                                diskLogFile = new DiskLogFile(latestFileName);
                                last = frontEnd.dbInterface.logger.getLatestLSN();
                            } catch (IOException io) {
                                throw new ReplicationException("The diskLogFile seems to be damaged. Reason: "+io.getMessage());
                            }
                        }
                        
                       // check the requested LSN for availability
                        if (last.getViewId()!=lsn.getViewId() || last.compareTo(lsn) < 0)
                            throw new ReplicationException("The requested LogEntry is not available, please load the Database. Last on Master: "+last.toString()+" Requested: "+lsn.toString()); 
                        
                       // parse the diskLogFile
                        while (diskLogFile.hasNext()) {
                            try {
                                if (le!=null) le.free();
                                le = diskLogFile.next();
                            } catch (LogEntryException e1) {
                                if (latestFileName.equals(frontEnd.dbInterface.logger.getLatestLogFileName())) {
                                    throw new ReplicationException("The requested LogEntry is not available, please load the Database.");
                                }
                                
                                // make an attempt to retry (logFile could have been switched)
                                try {
                                    latestFileName = frontEnd.dbInterface.logger.getLatestLogFileName();
                                    diskLogFile = new DiskLogFile(latestFileName);
                                    last = frontEnd.dbInterface.logger.getLatestLSN();
                                    
                                    if (last.getViewId()==lsn.getViewId() || last.compareTo(lsn)<0)
                                        throw new ReplicationException("The requested LogEntry is not available, please load the Database. Last on Master: "+last.toString()+" Requested: "+lsn.toString());
                                } catch (IOException io) {                                  
                                    throw new ReplicationException("The diskLogFile seems to be damaged. Reason: "+io.getMessage());
                                }
                            }
                            
                           // we hit the bullseye
                            if (le.getLSN().equals(lsn)){
                                try {
                                    buffer = le.serialize(checkSum);
                                    le.free();
                                } catch (IOException e) {
                                    throw new ReplicationException("The requested LogEntry is damaged. Reason: "+e.getMessage());
                                }
                                checkSum.reset();
                                orgReq.setResponse(HTTPUtils.SC_OKAY, buffer, DATA_TYPE.BINARY); 
                                notFound = false;
                                break;
                            }
                        }
                        
                       // requested LogEntry was not found 
                        if (notFound)
                            throw new ReplicationException("The requested LogEntry is not available anymore, please load the Database. Last on Master: "+last.toString()+" Requested: "+lsn.toString()); 
                        
                        break;
                     
                     // appreciates the ACK of a slave
                    case ACK :                   
                        if (slavesStatus.update(source.getAddress(),lsn))
                            if (frontEnd.dbInterface.dbCheckptr!=null) // otherwise the checkIntervall is 0
                                frontEnd.dbInterface.dbCheckptr.designateRecommendedCheckpoint(slavesStatus.getLatestCommonLSN());
                        break;
                    
                     // answers a LOAD request of a slave
                    case LOAD :                    
                     // Make the LSM-file metaData JSON compatible, for sending it to a slave
                        try {
                            File[] files = LSMDatabaseMOCK.getAllFiles();
                            Map<String,List<Long>> filesDetails = new Hashtable<String, List<Long>>();
                            for (File file : files) {
                                // TODO make a check of the fileName against the received LSN --> newRequest.getLSN();
                                
                                List<Long> parameters = new LinkedList<Long>();
                                parameters.add(file.length());
                                parameters.add(Replication.CHUNK_SIZE);
                                
                                filesDetails.put(file.getName(), parameters);
                            }
                        
                           // send these informations back to the slave    
                            buffer = ReusableBuffer.wrap(JSONParser.writeJSON(filesDetails).getBytes());
                            orgReq.setResponse(HTTPUtils.SC_OKAY, buffer, DATA_TYPE.JSON);
                        } catch (Exception e) {
                            throw new ReplicationException("LOAD_RQ could not be answered: '"+newRequest.toString()+"', because: "+e.getLocalizedMessage());
                        } 
                        break;                                                    
                        
                     // answers a chunk request of a slave
                    case CHUNK :              
                       // get the requested chunk                      
                        buffer = ReusableBuffer.wrap(
                                LSMDatabaseMOCK.getChunk(chunk.getFileName(),
                                chunk.getBegin(),chunk.getEnd()));                        
                        orgReq.setResponse(HTTPUtils.SC_OKAY, buffer, DATA_TYPE.BINARY);
                        break;   
                        
                  // slave's logic:
                        
                     // integrates a replica from the master
                    case REPLICA :            
                        LSN latestLSN = frontEnd.dbInterface.logger.getLatestLSN(); 
                        Status<LSN> mLSN = new Status<LSN>(lsn,PENDING);
                        if (missing.remove(mLSN)) missing.add(mLSN); 
                        
                        // check the sequence#
                        if (latestLSN.getViewId() == lsn.getViewId()) {
                            // put the logEntry and send ACK
                            if ((latestLSN.getSequenceNo()+1L) == lsn.getSequenceNo()) {
                                writeLogEntry(newRequest);
                                continue; // don't send a response at the end of the progress
                            // already in, so send ACK
                            } else if (latestLSN.getSequenceNo() >= lsn.getSequenceNo()) {
                                if (orgReq!=null) orgReq.setResponse(HTTPUtils.SC_OKAY);
                                else sendACK(newRequest);
                            // get the lost sequences and put it back on pending
                            } else {
                                LSN missingLSN = new LSN (latestLSN.getViewId(),latestLSN.getSequenceNo()+1L);
                                do {
                                    mLSN =  new Status<LSN>(missingLSN,OPEN);
                                    if (!missing.contains(mLSN) && !pending.contains(RequestPreProcessor.getExpectedREPLICA(missingLSN)))
                                        missing.add(mLSN);
                                    
                                    missingLSN = new LSN (missingLSN.getViewId(),missingLSN.getSequenceNo()+1L);
                                } while (missingLSN.compareTo(lsn)<0);
                                pending.add(newRequest);
                                continue; // don't send a response at the end of the progress
                            }
                        // get an initial copy from the master    
                        } else if (latestLSN.getViewId() < lsn.getViewId()){
                            sendLOAD(lsn);
                            pending.add(newRequest);
                            continue; // don't send a response at the end of the progress
                        } else {
                            if (orgReq!=null) orgReq.setResponse(HTTPUtils.SC_OKAY);
                            else sendACK(newRequest);
                            Logging.logMessage(Logging.LEVEL_WARN, this, "The Master seems to be out of order. Strange LSN received: '"+lsn.toString()+"'; latest LSN is: "+latestLSN.toString());
                        }
                        break;
                        
                     // evaluates the load details from the master
                    case LOAD_RP :
                       // make chunks and store them at the missingChunks                       
                       // for each file 
                        for (String fName : metaDataLSM.keySet()) {
                            Long fileSize = metaDataLSM.get(fName).get(0);
                            Long chunkSize = metaDataLSM.get(fName).get(1);
                            
                            assert (fileSize>0L);
                            assert (chunkSize>0L);
                            
                           // calculate chunks and add them to the list 
                            Long[] range = new Long[2];
                            range[0] = 0L;
                            for (long i=chunkSize;i<fileSize;i+=chunkSize) {
                                range[1] = i;
                                chunk = new Chunk(fName,range[0],range[1]);
                                missingChunks.add(new Status<Chunk>(chunk,OPEN));
                                range = new Long[2];
                                range[0] = i;
                            }
                            
                           // put the last chunk
                            if ((range[0]-fileSize)!=0L) {
                                chunk = new Chunk(fName,range[0],fileSize);
                                missingChunks.add(new Status<Chunk>(chunk,OPEN));
                            }
                        }                      
                        break;
                        
                     // saves a chunk sended by the master
                    case CHUNK_RP :                   
                        LSMDatabaseMOCK.writeFileChunk(chunk.getFileName(),data,chunk.getBegin(),chunk.getEnd());
                        break;
                        
                    default : 
                        assert(false) : "Unknown Request will be ignored: "+newRequest.toString();                       
                        throw new ReplicationException("Unknown Request will be ignored: "+newRequest.toString());
                    }
                    
                 // make a notification for a client/application if necessary   
                }catch (ReplicationException re){                    
                   // send a response to the client
                    if (orgReq!=null){
                        assert(!orgReq.responseSet) : "Response of the pinky request is already set! Request: "+newRequest.toString();                        
                        orgReq.setResponse(HTTPUtils.SC_SERVER_ERROR,re.getMessage());
                    } 
                    
                   // send a response to the application
                    if (context!=null){
                        context.getListener().requestFailed(context.getContext(), 
                                new BabuDBException(ErrorCode.REPLICATION_FAILURE, re.getMessage())); 
                    }
                 
                    Logging.logMessage(Logging.LEVEL_ERROR, this, re.getLocalizedMessage());
                }
                
                // send a pinky response
                sendResponse(orgReq);             
             // wait   
            } else {
                try {
                    Thread.sleep(TIMEOUT_GRANULARITY);          
                } catch (InterruptedException e) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "Waiting for work was interrupted.");
                }
            }
            
            synchronized (halt){
                while (halt.get() == true){
                    halt.notify();
                    try {
                        halt.wait();
                    } catch (InterruptedException e) {
                        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Waiting for context switch was interrupted.");
                    }
                }
            }           
        }
        
        if (running) notifyCrashed(new Exception("ReplicationThread crashed for an unknown reason!"));       
        notifyStopped();
    }

    /**
     * <p>Write the {@link LogEntry} from the given {@link Request} <code>req</code> to the DiskLogger and 
     * insert it into the LSM-tree.</p>
     *
     * @param req
     */
    private void writeLogEntry(Status<Request> req){
        try {
            int dbId = req.getValue().getContext().getInsertData().getDatabaseId();
            LSMDBWorker worker = frontEnd.dbInterface.getWorker(dbId);
            worker.addRequest(req.getValue().getContext());
        } catch (InterruptedException e) {
           // try again - if worker was interrupted
            req.setStatus(FAILED);
            pending.add(req);
            Logging.logMessage(Logging.LEVEL_ERROR, this, "LogEntry could not be written and will be put back into the pending queue," +
                    "\r\n\t because: "+e.getLocalizedMessage());
        }
    }
      
    /**
     * <p>Sends an {@link Token}.RQ for the given {@link Status} <code>lsn</code> to the master.<p>
     * <p>Sets the nextExpected flag to a higher priority, if necessary.</p>
     * 
     * @param lsn
     * @throws ReplicationException - if an error occurs.
     */
    private void sendRQ(Status<LSN> lsn) throws ReplicationException {
        try {          
            SpeedyRequest sReq = new SpeedyRequest(HTTPUtils.POST_TOKEN,Token.RQ.toString(),null,null,ReusableBuffer.wrap(lsn.getValue().toString().getBytes()),DATA_TYPE.BINARY);
            sReq.genericAttatchment = lsn;
            speedy.sendRequest(sReq, frontEnd.master);
            lsn.setStatus(PENDING);
        } catch (Exception e) {   
            lsn.setStatus(FAILED);
            missing.add(lsn);
            throw new ReplicationException("RQ ("+lsn.toString()+") could not be send to master: '"+frontEnd.master.toString()+"', because: "+e.getLocalizedMessage());           
        } 
        
        // reorder the missing lsn
        if (missing.remove(lsn))
            missing.add(lsn);
        
        // make a better restriction for the nextExpected request
        Request dummy = RequestPreProcessor.getExpectedREPLICA(lsn.getValue());
        if (hasHighPriority(new Status<Request>(dummy,OPEN))) setNextExpected(dummy);
    } 
    
    /**
     * <p>Sends an {@link Token}.CHUNK_RQ for the given <code>chunk</code> to the master.</p>
     * <p>Sets the nextExpected flag to a higher priority, if necessary.</p>
     * 
     * @param chunk
     * @throws ReplicationException - if an error occurs.
     */
    private void sendCHUNK(Status<Chunk> chunk) throws ReplicationException {
        try {           
            SpeedyRequest sReq = new SpeedyRequest(HTTPUtils.POST_TOKEN,
                    Token.CHUNK.toString(),null,null,
                    ReusableBuffer.wrap(JSONParser.writeJSON(chunk.getValue().toJSON()).getBytes()),DATA_TYPE.JSON);
            sReq.genericAttatchment = chunk;
            speedy.sendRequest(sReq, frontEnd.master);    
            chunk.setStatus(PENDING);
        } catch (Exception e) {   
            chunk.setStatus(FAILED);
            missingChunks.add(chunk);
            throw new ReplicationException("CHUNK ("+chunk.toString()+") could not be send to master: '"+frontEnd.master.toString()+"', because: "+e.getLocalizedMessage());           
        } 
        
        // reorder the missing chunks
        if (missingChunks.remove(chunk))
            missingChunks.add(chunk);
        
        // make a better restriction for the nextExpected request
        Request dummy = RequestPreProcessor.getExpectedCHUNK_RP(chunk.getValue());
        if (hasHighPriority(new Status<Request>(dummy,OPEN))) setNextExpected(dummy);
    }
    
    /**
     * <p>Sends an {@link Token}.LOAD to the given {@link LSN} <code>lsn</code> to the master.</p>
     * <p>Sets the nextExpected flag to a higher priority.</p>
     * 
     * @param lsn
     * @throws ReplicationException - if an error occurs.
     */
    private void sendLOAD(LSN lsn) throws ReplicationException{
        try {
            SpeedyRequest sReq = new SpeedyRequest(HTTPUtils.POST_TOKEN,Token.LOAD.toString(),null,null,ReusableBuffer.wrap(lsn.toString().getBytes()),DATA_TYPE.BINARY);
            speedy.sendRequest(sReq, frontEnd.master);
        } catch (Exception e) {
            throw new ReplicationException("LOAD till ("+lsn.toString()+") could not be send to master: '"+frontEnd.master.toString()+"', because: "+e.getLocalizedMessage());
        } 
        
        // make a better restriction for the nextExpected request
        setNextExpected(RequestPreProcessor.getExpectedLOAD_RP());
    }
    
    /**
     * <p>Used for shutting down the ReplicationThread, if a essential component crashed.</p>
     */
    private void violentShutdown() {
        running = false;
        if (pinky!=null && pinky.isAlive()) pinky.shutdown();
        if (speedy!=null && speedy.isAlive()) speedy.shutdown();      
    }
  
/*
 * shared functions
 */
    
    /**
     * <p>Wraps a Status around this request and enqueues it to the pending queue.</p>
     * 
     * @throws ReplicationException if the pending queue is full and the received request does not match the nextExpected request. 
     * @param rq - the request to enqueue.
     */
    void enqueueRequest(Request rq) throws ReplicationException{
        enqueueRequest(new Status(rq));
    }
    
    /**
     * <p>Enqueues an already wrapped request.</p>
     * 
     * @throws ReplicationException if the pending queue is full and the received request does not match the nextExpected request. 
     * @param rq - the request to enqueue
     */
    void enqueueRequest(Status<Request> rq) throws ReplicationException{
        // queue limit is reached
        if (pendingQueueLimit != 0 && pending.size()>pendingQueueLimit) {
            // if replication mechanism is >master< --> deny all requests
            if (frontEnd.isMaster()) 
                throw new ReplicationException("The pending queue is full. Master is too busy.");
            // if replication mechanism is >slave< --> deny all requests, but those which have a higher priority than or equal to the nextExpected
            else if (!hasHighPriority(rq))
                throw new ReplicationException("The pending queue is full. Next expected request to received will be: "+nextExpected.toString());
        }
        pending.add(rq);
    }
    
    /**
     * <p>Sends a synchronous request with JSON data attached to all available slaves.</p>
     * <p>Request will not be enqueued into the pending-queue!</p> 
     *     
     * @param rq
     * @throws ReplicationException
     */
    void sendSynchronousRequest(Request rq) throws ReplicationException{
        try {
            synchronized (frontEnd.slaves) {
                int count = frontEnd.slaves.size();
                rq.setMaxReceivableACKs(count);
                rq.setMinExpectableACKs(count);
                for (InetSocketAddress slave : frontEnd.slaves){
                    SpeedyRequest sReq = new SpeedyRequest(HTTPUtils.POST_TOKEN,rq.getToken().toString(),null,null,rq.getData(),DATA_TYPE.JSON);
                    sReq.genericAttatchment = rq;
                    speedy.sendRequest(sReq, slave);
                }    
                
                synchronized(rq) {
                    rq.wait();
                }
            }

            if (rq.failed()) throw new Exception("Operation failed!");
        } catch (Exception e) {
            throw new ReplicationException(rq.getToken().toString()+" could not be replicated. Because: "+e.getMessage());   
        }
    }
    
    /**
     * <p>Checks the {@link PinkyRequest} <code>rq</code> against <code>null</code>, before sending a Response.</p>
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
     * <p>Sends an {@link Token}.ACK for the given {@link LSN} <code>lsn</code> to the master.</p>
     * 
     * @param rq - for retrying purpose.
     * @throws ReplicationException - if an error occurs.
     */
    void sendACK(Status<Request> rq) throws ReplicationException{
        try {
            SpeedyRequest sReq = new SpeedyRequest(HTTPUtils.POST_TOKEN,Token.ACK.toString(),null,null,ReusableBuffer.wrap(rq.getValue().getLSN().toString().getBytes()),DATA_TYPE.BINARY);
            speedy.sendRequest(sReq, frontEnd.master);
        } catch (Exception e) {
            throw new ReplicationException("ACK ("+rq.getValue().getLSN().toString()+") could not be send to master: '"+frontEnd.master.toString()+"', because: "+e.getLocalizedMessage());   
        } 
    }
    
    /**
     * <p>Approach to remove missing LSNs if the given <code>rq</code> was written to the BabuDB.</p>
     * 
     * @param rq
     */
    void removeMissing(Status<Request> rq){
        missing.remove(new Status<LSN>(rq.getValue().getLSN()));
        if (hasHighPriority(rq)) resetNextExpected();
    }
    
    /**
     * <p>Changes the replication mechanism behavior to master.</p>
     */
    void toMaster(){
        missing = null;
        missingChunks = null;
        if (slavesStatus == null)
            slavesStatus = new SlavesStatus(frontEnd.slaves);
    }
    
    /**
     * <p>Changes the replication mechanism behavior to slave.</p>
     */
    void toSlave(){
        if (missing == null)
            missing = new PriorityBlockingQueue<Status<LSN>>();
        if (missingChunks == null)
            missingChunks = new PriorityBlockingQueue<Status<Chunk>>();
        slavesStatus = null;
    }
    
    /**
     * <p>Shuts the {@link ReplicationThread} down gracefully. All connections are closed.</p>
     * @throws Exception 
     */
    void shutdown() throws Exception {
        running = false;
        pinky.shutdown();
        speedy.shutdown();
        pinky.waitForShutdown();
        speedy.waitForShutdown();        
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
        String message = "";
        if (frontEnd.isMaster())
            message = "A component of the Master ReplicationThread crashed. The ReplicationThread will be shut down.";
        else
            message = "A component of the Slave ReplicationThread crashed. The ReplicationThread will be shut down.";
        Logging.logMessage(Logging.LEVEL_ERROR, this, message);       
        violentShutdown();
    }


    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() {
        String message = "";
        if (frontEnd.isMaster())
            message = "Master ReplicationThread component stopped.";
        else
            message = "Slave ReplicationThread component stopped.";
        Logging.logMessage(Logging.LEVEL_INFO, this, message);      
    }


    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() {
        String message = "";
        if (frontEnd.isMaster())
            message = "Master ReplicationThread component started.";
        else
            message = "Slave ReplicationThread component started.";
        Logging.logMessage(Logging.LEVEL_INFO, this, message);  
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        Logging.logMessage(Logging.LEVEL_ERROR, this, "Critical error in thread: "+t.getName()+" because: "+e.getLocalizedMessage());
    }  
    
/*
 * Getter/Setter for the 'nextExpected' flag
 */
    
    /**
     * @return true, if the given request has a higher, or equal priority than the nextExpected request, false otherwise.
     */
    private synchronized boolean hasHighPriority(Status<Request> rq) {
        assert (rq!=null);
        return rq.compareTo(nextExpected) <= 0;
    }
    
    /**
     * @param rq - wraps Status around the request and sets it as nextExpected.
     */
    private synchronized void setNextExpected(Request rq) {
        this.nextExpected = new Status<Request>(rq);
    }
    
    /**
     * <p>Sets the nextExpected to the lowest priority.</p>
     */
    private synchronized void resetNextExpected() {
        this.nextExpected = LOWEST_PRIORITY_REQUEST;
    }
}
