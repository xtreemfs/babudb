/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.BabuDBConfiguration;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogFile;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.Replication.CONDITION;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.LifeCycleListener;
import org.xtreemfs.include.foundation.LifeCycleThread;
import org.xtreemfs.include.foundation.json.JSONException;
import org.xtreemfs.include.foundation.json.JSONParser;
import org.xtreemfs.include.foundation.pinky.HTTPUtils;
import org.xtreemfs.include.foundation.pinky.PinkyRequest;
import org.xtreemfs.include.foundation.pinky.PipelinedPinky;
import org.xtreemfs.include.foundation.pinky.SSLOptions;
import org.xtreemfs.include.foundation.pinky.HTTPUtils.DATA_TYPE;
import org.xtreemfs.include.foundation.speedy.MultiSpeedy;
import org.xtreemfs.include.foundation.speedy.SpeedyRequest;

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
    
    /** <p>Milliseconds idle-time, if the pending and the missing queues are empty.</p> */
    final static int TIMEOUT_GRANULARITY = 250;
    
    /** <p>Requests that are waiting to be done.</p> */
    private final PriorityBlockingQueue<Status<Request>> pending;
    
    /** <p>LSN's of missing {@link LogEntry}s.</p> */
    private PriorityBlockingQueue<Status<LSN>> missing;
    
    /** <p>Details for missing file-chunks. File Name + [chunk-beginL,chunk-endL].</p> */
    private PriorityBlockingQueue<Status<Chunk>> missingChunks;
    
    /**
     * <p>Table of slaves with their latest acknowledged {@link LSN}s.</p>
     */
    final SlavesStatus slavesStatus;
    
    /** <p>Pinky - Thread.</p> */
    private final PipelinedPinky pinky;
    
    /** <p> {@link MultiSpeedy} - Thread.</p> */
    private final MultiSpeedy speedy;
    
    /** <p>Holds all variable configuration parameters and is the approach for the complete replication.</p> */
    private final Replication frontEnd;
    
    /** <p>Flag which implies, that this {@link LifeCycleThread} is running.</p> */
    private boolean running = false;

    /**
     * <p>Flag which implies the Thread to halt and notify the waiting application.</p>
     */
    final AtomicBoolean halt = new AtomicBoolean(false);
    
    /** <p>Lazy holder for the diskLogFile.</p> */
    private DiskLogFile diskLogFile = null;
    
    /** <p>A request dummy with the lowest priority for the pending queue.</p> */
    private static final Status<Request> LOWEST_PRIORITY_REQUEST = new Status<Request>();
    
    /**
     * <p>If a request could not be processed it will be noticed to wait for newer entries for example.
     * With that polling on the same Request is avoided.</p> 
     * <p>If the pendingQueueLimit is reached, requests with a lower priority than the nextExpected will be aborted.</p>
     */
    private final AtomicReference<Status<Request>> nextExpected = new AtomicReference<Status<Request>>(LOWEST_PRIORITY_REQUEST);
    
    /** <p>Maximal number of {@link Request} to store in the pending queue.</p> */
    private final int pendingQueueLimit;
    
    /** <p>The default comparator for the order at the pending queue.</p> */
    private final PriorityQueueComparator<Request> PENDING_QUEUE_COMPARATOR = new PriorityQueueComparator<Request>();
    
    /**
     * <p>Default constructor. Synchronous start-up for pinky and speedy component.</p>
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
        pending = new PriorityBlockingQueue<Status<Request>>(100,PENDING_QUEUE_COMPARATOR);
        missing = (!frontEnd.isMaster()) ? new PriorityBlockingQueue<Status<LSN>>(100,new PriorityQueueComparator<LSN>()) : null;
        missingChunks = (!frontEnd.isMaster()) ? new PriorityBlockingQueue<Status<Chunk>>(10,new PriorityQueueComparator<Chunk>()) : null;
        this.slavesStatus = new SlavesStatus(frontEnd.slaves);

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
            String msg = "ReplicationThread did not start properly, because: "+io.getMessage();
            Logging.logMessage(Logging.LEVEL_ERROR, this, msg);
            crashPerformed();
        }  
    }
    
    /**
     * <p>Work the requestQueue <code>pending</code>.</p>
     */
    @Override
    public void run() {  
        final Checksum checkSum = new CRC32(); 
        Chunk chunk;
        LSMDBRequest context;
        InetSocketAddress source;
        PinkyRequest orgReq;            
        byte[] data;
        LSN lsn;
        Map<String, List<Long>> metaDataLSM;
        boolean idle;
        
        super.notifyStarted();
        while (running) {
          //initializing...
            chunk = null;
            context = null;
            source = null;
            orgReq = null;            
            data = null;
            lsn = null;
            metaDataLSM = null;
            idle = true;
            
          // check for missing LSNs and missing file chunks -highest priority- (just for slaves)          
            if (!frontEnd.isMaster()) {
                try{
                   // if there are any missing file chunks
                    Status<Chunk> missingChunk = missingChunks.peek();
                    if (missingChunk != null && !missingChunk.isPending()) {
                        missingChunk.pending();
                        idle = false;
                        try {
                            sendCHUNK(missingChunk);     
                        } catch (Exception e) {
                        	missingChunk.failed(e.getMessage(), frontEnd.maxTries);
                        		
                        }
                        
                    } else {                   
	                   // if there are any missing logEntries
	                    Status<LSN> missingLSN = missing.peek();
	                    if (!frontEnd.getCondition().equals(CONDITION.LOADING) && missingLSN != null && !missingLSN.isPending()){
	                        missingLSN.pending();
	                        idle = false;
	                        try {
	                            sendRQ(missingLSN);
	                        } catch (Exception e) {
	                        	missingLSN.failed(e.getMessage(), frontEnd.maxTries);
	                        }
	                    }
                    }
                // missing LSN/Chunk - request failed    
                }catch (ReplicationException re){
                    String msg = "The master seems to be not available for information-retrieval: "+re.getMessage();
                    Logging.logMessage(Logging.LEVEL_ERROR, this, msg);
                    frontEnd.dbInterface.replication_runtime_failure(msg);
                }
            }  
          // get a new request 
            Status<Request> newRequest = pending.peek();     
            if (newRequest != null && hasHighPriority(newRequest) && !newRequest.isPending()) {              
                try{
                    newRequest.pending();
                    resetNextExpected();
                    idle = false;
                	
                    chunk = newRequest.getValue().getChunkDetails();
                    source = newRequest.getValue().getSource();
                    orgReq = newRequest.getValue().getOriginal();
                    context = newRequest.getValue().getContext();
                    data = newRequest.getValue().getData();
                    lsn = newRequest.getValue().getLSN();
                    metaDataLSM = newRequest.getValue().getLsmDbMetaData();
                    
                    switch(newRequest.getValue().getToken()) {
                    
                  // master's logic:
                    
                     // sends logEntry to the given destinations. as a broadcast, the thread has to continue, without marking the request as finished
                    case REPLICA_BROADCAST :   
                        frontEnd.updateLastWrittenLSN(lsn);                       
                        if (sendBROADCAST(newRequest,Token.REPLICA)) break;
                        continue;
    
                     // sends a CREATE to the given destinations. as a broadcast, the thread has to continue, without marking the request as finished
                    case CREATE: 
                        if (sendBROADCAST(newRequest, Token.CREATE)) break;
                        continue;
                     
                     // sends a CREATE to the given destinations. as a broadcast, the thread has to continue, without marking the request as finished   
                    case COPY: 
                        if (sendBROADCAST(newRequest, Token.COPY)) break;
                        continue;
                        
                     // sends a CREATE to the given destinations. as a broadcast, the thread has to continue, without marking the request as finished   
                    case DELETE: 
                        if (sendBROADCAST(newRequest, Token.DELETE)) break;
                        continue;
                     
                     // sends a STATE to the given destinations. as a broadcast, the thread has to continue, without marking the request as finished     
                    case STATE_BROADCAST : 
                    	if (sendBROADCAST(newRequest, Token.STATE)) break;
                    	continue;
                        
                     // answers a slaves logEntry request
                    case RQ : 
                        boolean notFound = true;
                        LogEntry le = null;
                        String latestFileName;
                      
                        frontEnd.babuDBLockcontextSwitchLock.lock();
                       // get the latest logFile
                        try {
                            latestFileName = frontEnd.dbInterface.logger.getLatestLogFileName();
                            diskLogFile = new DiskLogFile(latestFileName);     
                        } catch (IOException e) {
                            frontEnd.babuDBLockcontextSwitchLock.unlock();
                            throw new ReplicationException("The diskLogFile seems to be damaged. Reason: "+e.getMessage());
                        }
                        
                        LSN last = frontEnd.dbInterface.logger.getLatestLSN();
                       // check the requested LSN for availability
                        if (last.getViewId()!=lsn.getViewId() || last.compareTo(lsn) < 0) {
                            frontEnd.babuDBLockcontextSwitchLock.unlock();
                            try {
                                diskLogFile.close();
                            }catch (IOException ioe) { /* ignored in this case */ }
                            orgReq.setResponse(HTTPUtils.SC_NOT_FOUND,"The requested LogEntry is not available, please load the Database. Last on Master: "+last.toString()+" Requested: "+lsn.toString());
                            break;
                        }
                        
                       // parse the diskLogFile
                        while (diskLogFile.hasNext()) {
                            try {
                                if (le!=null) le.free();
                                le = diskLogFile.next();
                            } catch (LogEntryException e) {
                                if (le!=null) le.free();
                                try {
                                    diskLogFile.close();
                                }catch (IOException ioe) { /* ignored in this case */ }
                                frontEnd.babuDBLockcontextSwitchLock.unlock();
                                throw new ReplicationException("The requested LogEntry is not available, please load the Database. Reason: "+e.getMessage());
                            }
                            
                           // we hit the bullseye
                            if (le.getLSN().equals(lsn)){
                                try {
                                    orgReq.setResponse(HTTPUtils.SC_OKAY, le.serialize(checkSum), DATA_TYPE.BINARY);
                                    notFound = false;
                                    checkSum.reset();
                                    le.free();
                                } catch (IOException e) {
                                    checkSum.reset();
                                    le.free();
                                    try {
                                        diskLogFile.close();
                                    }catch (IOException ioe) { /* ignored in this case */ }
                                    frontEnd.babuDBLockcontextSwitchLock.unlock();
                                    throw new ReplicationException("The requested LogEntry is damaged. Reason: "+e.getMessage());
                                }  
                                break;
                            } 
                        }
                        
                       // requested LogEntry was not found 
                        if (notFound) {
                            try {
                                diskLogFile.close();
                            }catch (IOException ioe) { /* ignored in this case */ }
                            frontEnd.babuDBLockcontextSwitchLock.unlock();
                            orgReq.setResponse(HTTPUtils.SC_NOT_FOUND,"The requested LogEntry is not available. Load the DB.");
                            break; 
                        }
                        
                        try {
                            diskLogFile.close();
                        }catch (IOException ioe) { /* ignored in this case */ }                       
                        frontEnd.babuDBLockcontextSwitchLock.unlock();
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
                            Map<String,List<Long>> filesDetails = new Hashtable<String, List<Long>>();
                            String path;
                            long length;
                            
                             // add the DB-structure-file metadata
                            path = frontEnd.dbInterface.getDBConfigPath();
                            length = new File(path).length();
                            
                            List<Long> parameters = new LinkedList<Long>();
                            parameters.add(length);
                            parameters.add(Replication.CHUNK_SIZE);
                            filesDetails.put(path, parameters);
                           
                             // add the latest snapshot files for every DB, if available
                            for (LSMDatabase db : frontEnd.dbInterface.databases.values())
                                filesDetails.putAll(db.getLastestSnapshotFiles());
                            
                             // send these informations back to the slave    
                            orgReq.setResponse(HTTPUtils.SC_OKAY, ReusableBuffer.wrap(JSONParser.writeJSON(filesDetails).getBytes()), DATA_TYPE.JSON);
                        } catch (Exception e) {      
                        	throw new ReplicationException("LOAD_RQ could not be answered: '"+newRequest.toString()+"', because: "+e.getMessage());
                        } 
                        break;                                                    
                        
                     // answers a chunk request of a slave
                    case CHUNK :           
                        try{
                            int length = (int) (chunk.getEnd() - chunk.getBegin());
                            byte[] bbuf = new byte[length];
                            File f = new File(chunk.getFileName());
                            if (!f.exists()) {
                                Logging.logMessage(Logging.LEVEL_INFO, this, "A requested chunk does not exist anymore: "+chunk.toString());
                                orgReq.setResponse(HTTPUtils.SC_NOT_FOUND, "The requested chunk does not exist anymore!");
                                break;
                            }
                            if (f.length()<length)
                                throw new Exception("File length was too small: "+length+"b requested, "+f.length()+"b available.");
                            FileInputStream in = new FileInputStream(f);
                             // get the requested chunk 
                            in.read(bbuf, ((int) chunk.getBegin()), length);
                                         
                            orgReq.setResponse(HTTPUtils.SC_OKAY, ReusableBuffer.wrap(bbuf), DATA_TYPE.BINARY);
                        } catch (Exception e){     
                        	throw new ReplicationException("CHUNK request could not be answered: '"+newRequest.toString()+"', because: "+e.getMessage());
                        }
                        break;   
                        
                  // slave's logic:
                     
                     // send a LOAD to the master
                    case LOAD_RQ :                       
                        // delete all previews loaded files 
                        if (frontEnd.getCondition().equals(CONDITION.LOADING)) {
                            frontEnd.switchCondition(CONDITION.RUNNING);
                            
                            Status<Chunk> c=null;
                            while ((c = missingChunks.poll())!=null) {
                                try {
                                    getFile(c.getValue()).delete();
                                }catch (IOException e) { /* ignored in this case */ }
                            } 
                        }   
                        try {
                            sendLOAD(newRequest);
                        } catch (Exception e){
                        	throw new ReplicationException("LOAD could not be send to master: '"+frontEnd.master.toString()+"', because: "+e.getMessage());
                        }
                        break;
                        
                     // integrates a replica from the master
                    case REPLICA :   
                        // wait if loading is in progress
                        if (frontEnd.getCondition().equals(CONDITION.LOADING)){
                            newRequest.retry();
                            continue; // don't send a response at the end of the progress
                        }
                        try {
                             // preparing the request...
                            context = retrieveRequest(newRequest); 
                            
                            LSN latestLSN = frontEnd.getLastWrittenLSN(); 
                            Status<LSN> mLSN = new Status<LSN>(lsn,this);
                            if (missing.contains(mLSN)) mLSN.pending();
                            
                             // check the sequence#
                            if (latestLSN.getViewId() == lsn.getViewId()) {
                                 // put the logEntry and send ACK
                                if ((latestLSN.getSequenceNo()+1L) == lsn.getSequenceNo()) {
                                    try {
                                        writeLogEntry(newRequest,context);
                                    } catch (InterruptedException e) {
                                    	throw new ReplicationException("LogEntry could not be written and will" +
                                    			" be put back into the pending queue,"+
                                                "\r\n\t because: "+e.getMessage());
                                    }
                                    continue; // don't send a response at the end of the progress
                                 // get the lost sequences and put it back on pending
                                } else if (latestLSN.getSequenceNo() < lsn.getSequenceNo()) {
                                    LSN missingLSN = new LSN (latestLSN.getViewId(),latestLSN.getSequenceNo()+1L);
                                    do {
                                        mLSN =  new Status<LSN>(missingLSN,this);
                                        if (!missing.contains(mLSN) && !pending.contains(new Status<Request>(RequestPreProcessor.getExpectedREPLICA(missingLSN))))
                                            missing.add(mLSN);
                                        
                                        missingLSN = new LSN (missingLSN.getViewId(),missingLSN.getSequenceNo()+1L);
                                    } while (missingLSN.compareTo(lsn)<0);   
                                    newRequest.retry();
                                    continue; // don't send a response at the end of the progress
                                }
                            // get an initial copy from the master    
                            } else if (latestLSN.getViewId() < lsn.getViewId()){
                                Status<Request> loadRq = new Status<Request> (RequestPreProcessor.getLOAD_RQ(),this);
                                try {
                                    sendLOAD(loadRq);
                                    Logging.logMessage(Logging.LEVEL_INFO, this, "LOAD was send successfully.");
                                } catch (Exception e) {
                                	throw new ReplicationException("Load could not be send.");
                                }  
                                newRequest.retry();
                                continue; // don't send a response at the end of the progress
                            } else {
                                Logging.logMessage(Logging.LEVEL_WARN, this, "The Master seems to be out of order. Strange LSN received: '"+lsn.toString()+"'; latest LSN is: "+latestLSN.toString());
                            }                           
                        } catch (IOException ioe) {
                            Status<Request> loadRq = new Status<Request> (RequestPreProcessor.getLOAD_RQ(),this);
                            try {
                                sendLOAD(loadRq);
                                Logging.logMessage(Logging.LEVEL_INFO, this, ioe.getMessage()+" - LOAD was send successfully.");
                            } catch (Exception e) {
                            	throw new ReplicationException("Load could not be send.");
                            }
                            newRequest.retry();
                            continue; // don't send a response at the end of the progress
                        }
                        break;
                        
                     // evaluates the load details from the master
                    case LOAD_RP :                        
                       // make chunks and store them at the missingChunks                       
                       // for each file 
                        for (String fName : metaDataLSM.keySet()) {
                            Long fileSize = metaDataLSM.get(fName).get(0);
                            Long chunkSize = metaDataLSM.get(fName).get(1);
                            
                            assert (fileSize>0L) : "Empty files are not allowed.";
                            assert (chunkSize>0L) : "Empty chunks are not allowed.";
                            
                           // calculate chunks and add them to the list 
                            Long[] range = new Long[2];
                            range[0] = 0L;
                            for (long i=chunkSize;i<fileSize;i+=chunkSize) {
                                range[1] = i;
                                chunk = new Chunk(fName,range[0],range[1]);
                                missingChunks.add(new Status<Chunk>(chunk,this));
                                Logging.logMessage(Logging.LEVEL_TRACE, this, "Chunk added: "+chunk.toString());
                                range = new Long[2];
                                range[0] = i;
                            }
                            
                           // put the last chunk
                            if ((range[0]-fileSize)!=0L) {
                                chunk = new Chunk(fName,range[0],fileSize);
                                missingChunks.add(new Status<Chunk>(chunk,this));
                                Logging.logMessage(Logging.LEVEL_TRACE, this, "Chunk added: "+chunk.toString());
                            } 
                            
                           // change the latest LSN to the one expected after loading
                            File f = new File(fName);
                            fName = f.getName();
                            if (LSMDatabase.isSnapshotFilename(fName)) frontEnd.updateLastWrittenLSN(LSMDatabase.getSnapshotLSNbyFilename(fName));
                        }    
                        // make a higher restriction for the nextExpected request
                        setNextExpected(RequestPreProcessor.getExpectedCHUNK_RP(chunk));
                        break;
                        
                     // saves a chunk send by the master
                    case CHUNK_RP :      
                        int length = (int) (chunk.getEnd() - chunk.getBegin());
                                               
                        try {
                             // insert the file input
                            FileOutputStream fO = new FileOutputStream(getFile(chunk));
                            fO.write(data, (int) chunk.getBegin(), length);
                        } catch (Exception e) {
                        	throw new ReplicationException("Chunk could not be written to disk: "+chunk.toString()+"\n"+
                                    "because: "+e.getMessage());
                        }
                        Logging.logMessage(Logging.LEVEL_TRACE, this, "Chunk written: "+chunk.toString());                  
                        break;
                        
                  // shared logic
                        
                     // send the LSN for the latest written LogEntry as Response   
                    case STATE :                                       
                        orgReq.setResponse(HTTPUtils.SC_OKAY, 
                                ReusableBuffer.wrap(frontEnd.getLastWrittenLSN().toString().getBytes()), DATA_TYPE.BINARY);
                        break;
                        
                    default : 
                        assert(false) : "Unknown Request will be ignored: "+newRequest.toString();                       
                        throw new ReplicationException("Unknown Request will be ignored: "+newRequest.toString());
                    }
                     
                    // request finished
                    newRequest.finished();
                 // request failed
                }catch (ReplicationException re){   
                	try {
                		newRequest.failed(re.getMessage(), frontEnd.maxTries);
                	} catch (ReplicationException e) {
                		Logging.logMessage(Logging.LEVEL_ERROR, this, "Request did not fail properly: "+e.getMessage());
                	}
                }     
            } 
                       
            // halt, if necessary
            synchronized (halt){
                while (halt.get() == true){
                    halt.notify();
                    try {
                        halt.wait();
                    } catch (InterruptedException e) {
                        Logging.logMessage(Logging.LEVEL_WARN, this, "Waiting for context switch was interrupted.");
                    }
                }
            }  
            
            // sleep, if idle 
            if (idle) {
                try {
                    Thread.sleep(TIMEOUT_GRANULARITY);          
                } catch (InterruptedException e) {
                    Logging.logMessage(Logging.LEVEL_WARN, this, "Waiting for work was interrupted.");
                }
            }      
        }
        
        if (running) notifyCrashed(new Exception("ReplicationThread crashed for an unknown reason!"));       
        notifyStopped();
    }
    
    /**
     * @param rq
     * @return the LSMDBRequest retrieved from the logEntry
     * @throws IOException - if the DB is not consistent and should be loaded.
     */
    private LSMDBRequest retrieveRequest (Status<Request> rq) throws IOException{
        // build a LSMDBRequest
        ReusableBuffer buf = rq.getValue().getLogEntry().getPayload().createViewBuffer();
        InsertRecordGroup irg = InsertRecordGroup.deserialize(buf);
        BufferPool.free(buf);
        
        if (!frontEnd.dbInterface.databases.containsKey(irg.getDatabaseId()))
            throw new IOException("Database does not exist.Load DB!");
        
        return new LSMDBRequest(frontEnd.dbInterface.databases.get(irg.getDatabaseId()),
                                frontEnd,irg,rq);
    }
    
    /**
     * <p>Write the {@link LogEntry} given by a generated {@link LSMDBRequest} <code>context</code> from the 
     * given {@link Request} <code>rq</code> to the DiskLogger and insert it into the LSM-tree.</p>
     * 
     * @param context
     * @param rq - the original request
     * @throws InterruptedException - if an error occurs.
     */
    private void writeLogEntry(Status<Request> rq,LSMDBRequest context) throws InterruptedException{
        int dbId = context.getInsertData().getDatabaseId();
        LSMDBWorker worker = frontEnd.dbInterface.getWorker(dbId);
        worker.addRequest(context);
    }
      
    /**
     * <p>Sends an {@link Token}.RQ for the given {@link Status} <code>lsn</code> to the master.<p>
     * <p>Sets the nextExpected flag to a higher priority, if necessary.</p>
     * 
     * @param rq
     * @throws IOException - if an error occurs.
     * @throws IllegalStateException - if an error occurs.

     */
    private void sendRQ(Status<LSN> rq) throws IllegalStateException, IOException { 
        SpeedyRequest sReq = new SpeedyRequest(HTTPUtils.POST_TOKEN,Token.RQ.toString(),null,null,ReusableBuffer.wrap(rq.getValue().toString().getBytes()),DATA_TYPE.BINARY);
        sReq.genericAttatchment = rq;
        speedy.sendRequest(sReq, frontEnd.master);
        
        // make a better restriction for the nextExpected request
        Request dummy = RequestPreProcessor.getExpectedREPLICA(rq.getValue());
        if (hasHighPriority(new Status<Request>(dummy,this))) setNextExpected(dummy);
    } 
    
    /**
     * <p>Sends an {@link Token}.LOAD request to the master.</p>
     * <p>Sets the nextExpected flag to a higher priority.</p>
     * 
     * @param rq - the original {@link Request}.
     * @throws IOException - if an error occurs.
     * @throws IllegalStateException - if an error occurs.
     */
    private void sendLOAD(Status<Request> rq) throws IllegalStateException, IOException {     
        frontEnd.switchCondition(CONDITION.LOADING);
        SpeedyRequest sReq = new SpeedyRequest(HTTPUtils.POST_TOKEN,Token.LOAD.toString(),null,null,null,DATA_TYPE.BINARY);
        sReq.genericAttatchment = rq;
        speedy.sendRequest(sReq, frontEnd.master);
        
        // make a higher restriction for the nextExpected request
        setNextExpected(RequestPreProcessor.getExpectedLOAD_RP());            
    }
    
    /**
     * <p>Sends an {@link Token}.CHUNK_RQ for the given <code>chunk</code> to the master.</p>
     * <p>Sets the nextExpected flag to a higher priority, if necessary.</p>
     * 
     * @param chunk
     * @throws JSONException - if an error occurs.
     * @throws IOException - if an error occurs.
     * @throws IllegalStateException - if an error occurs.
     */
    private void sendCHUNK(Status<Chunk> chunk) throws JSONException, IllegalStateException, IOException { 
        SpeedyRequest sReq = new SpeedyRequest(HTTPUtils.POST_TOKEN,
                Token.CHUNK.toString(),null,null,
                ReusableBuffer.wrap(JSONParser.writeJSON(chunk.getValue().toJSON()).getBytes()),DATA_TYPE.JSON);
        sReq.genericAttatchment = chunk;
        speedy.sendRequest(sReq, frontEnd.master);    
        
        // make a higher restriction for the nextExpected request
        Request dummy = RequestPreProcessor.getExpectedCHUNK_RP(chunk.getValue());
        if (hasHighPriority(new Status<Request>(dummy,this))) setNextExpected(dummy);            
    }
    
    /**
     * <p>Sends a {@link Request} <code>rq</code> as broadcast to all available slaves.</p>
     * 
     * @param rq
     * @throws ReplicationException if an error occurs.
     * @return true, if the thread shall break, false if it shall continue.
     */
    private boolean sendBROADCAST(Status<Request> rq,Token t) throws ReplicationException {
        int replicasFailed = 0;
            final int slaveCount = rq.getValue().getDestinations().size();
            rq.setMaxReceivableResp(slaveCount);
            rq.setMinExpectableResp(frontEnd.syncN);
            
            for (InetSocketAddress slave : rq.getValue().getDestinations()) {
                // build the request
                SpeedyRequest srq = new SpeedyRequest(HTTPUtils.POST_TOKEN,
                                                     t.toString(),null,null,
                                                     ReusableBuffer.wrap(rq.getValue().getData()),
                                                     DATA_TYPE.BINARY);
                srq.genericAttatchment = rq;
                
                // send the request
                try{  
                    speedy.sendRequest(srq, slave); 
                } catch (Exception e){
                    srq.freeBuffer();
                    replicasFailed++;
                    Logging.logMessage(Logging.LEVEL_WARN, this, t.toString()+" could not be send to slave: '"+slave.toString()+"', because: "+e.getMessage());
                }
            }
            
             // ASYNC mode unavailable slaves will be ignored for the response   
            if(frontEnd.syncN > 0){ 
	            String msg = "The replication of '"+rq.getValue().getToken().toString()+"' was not successful. " +
	                         "'"+replicasFailed+"' of '"+slaveCount+"' slaves could not be reached.";
	            
	            if (replicasFailed > 0)
	                for (int i=0;i<replicasFailed;i++)
	                    rq.failed(msg,frontEnd.maxTries);
	            
	            return false;
            }else
            	return true;
    }
    
    /**
     * @param chunk
     * @return the {@link File} to the given {@link Chunk}.
     * @throws IOException
     */
    private File getFile(Chunk chunk) throws IOException{
        File chnk = new File(chunk.getFileName());
        String fName = chnk.getName();
        File result;
        
        if (LSMDatabase.isSnapshotFilename(fName)) {
             // create the db-name directory, if necessary
            new File(frontEnd.dbInterface.configuration.getBaseDir() + 
                    chnk.getParentFile().getName() + File.separatorChar).mkdirs();
             // create the file if necessary
            result = new File(frontEnd.dbInterface.configuration.getBaseDir() + 
                    chnk.getParentFile().getName() + File.separatorChar + fName);
            result.createNewFile();
        } else if (chnk.getParent() == null){
             // create the file if necessary
            result = new File(frontEnd.dbInterface.configuration.getBaseDir() + BabuDBConfiguration.DBCFG_FILE);
            result.createNewFile();
        } else{
             // create the file if necessary
            result = new File(frontEnd.dbInterface.configuration.getDbLogDir() + fName);
            result.createNewFile();
        }
        return result;
    }
    
/*
 * shared functions
 */
    
    /**
     * <p>Enqueues an already wrapped request.</p>
     * 
     * @throws ReplicationException if the pending queue is full and the received request does not match the nextExpected request. 
     * @param rq - the request to enqueue
     * @return true, if the request was successful enqueued, false if it was already in the queue.
     */
    boolean enqueueRequest(Status<Request> rq) throws ReplicationException{
        if (pending.contains(rq)) return false;

        // queue limit is reached
        if (pendingQueueLimit != 0 && pending.size()>pendingQueueLimit) {
            // if replication mechanism is >master< --> deny all requests
            if (frontEnd.isMaster()) 
                throw new ReplicationException("The pending queue is full. Master is too busy.\nRejected: "+rq.toString());
            // if replication mechanism is >slave< --> deny all requests, but those which have a higher priority than or equal to the nextExpected
            else if (!hasHighPriority(rq))
                throw new ReplicationException("The pending queue is full. Next expected request to received will be: "+nextExpected.toString()+"\nRejected: "+rq.toString());
        }    
        
        pending.add(rq);
        Logging.logMessage(Logging.LEVEL_ERROR, this, "ADDED: "+rq.toString());
        return true;
    }

    /**
     * <p>Wraps a Status around this request and enqueues it to the pending queue.</p>
     * 
     * @throws ReplicationException if the pending queue is full and the received request does not match the nextExpected request. 
     * @param rq - the request to enqueue.
     * @return true, if the request was successful enqueued, false if it was already in the queue.
     */
    boolean enqueueRequest(Request rq) throws ReplicationException{      
        return enqueueRequest(new Status<Request>(rq,this));
    }
    
    /**
     * <p>Removes requests from the queues of the {@link ReplicationThread}.</p>
     * <p>Resets the nextExpected, if necessary.</p>
     * 
     * @param <T>
     * @param rq
     * @throws ReplicationException 
     */
    @SuppressWarnings("unchecked")
    <T> void remove(Status<T> rq) throws ReplicationException {
        Logging.logMessage(Logging.LEVEL_TRACE, this, "Removing: "+rq.toString());
        if (rq.getValue() instanceof Request) {
            pending.remove((Status<Request>) rq);
            Chunk chunk = ((Request)rq.getValue()).getChunkDetails();
            
            if (hasHighPriority((Status<Request>) rq)) resetNextExpected();
            if (missing!=null && missing.remove(new Status<LSN>(((Request)rq.getValue()).getLSN()))) resetNextExpected();
            if (missingChunks!=null && chunk!=null){
            	if (missingChunks.remove(new Status<Chunk>(chunk))) resetNextExpected();
                if (missingChunks.isEmpty()){ 
                    try {
                        LSN lsn = frontEnd.getLastWrittenLSN();
                        frontEnd.dbInterface.reset(lsn);
                        frontEnd.switchCondition(CONDITION.RUNNING);
                        Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB was reseted successfully. New latest LSN: "+lsn.toString()); 
                    } catch (BabuDBException e) {
                        throw new ReplicationException("BabuDB could not be reseted. The system is properly inconsistent and not running at the moment! :"+e.getMessage());
                    }  
                    // make a restriction for the nextExpected request
                } else {
                	setNextExpected(new Status<Request>(RequestPreProcessor.getExpectedCHUNK_RP(chunk),true));
                }
            }         
            ((Request) rq.getValue()).free();
        } else if (rq.getValue() instanceof LSN) {
            missing.remove((Status<LSN>) rq);
        } else if (rq.getValue() instanceof Chunk) {
            missingChunks.remove((Status<Chunk>) rq);
        } else
            throw new ReplicationException("Malformed request: "+rq.toString());     
    }
    
    /**
     * <p>Notifies available listeners for the finishing of the given {@link Request} and removes it from the queues.</p>
     * 
     * @param <T>
     * @param rq
     * @throws ReplicationException if the {@link Request} could not be removed properly. 
     */
    <T> void finished(Status<T> rq) throws ReplicationException {
        if (rq.getValue() instanceof Request) {
            Request req = (Request) rq.getValue();
            PinkyRequest pinkyRq = req.getOriginal();
            LSMDBRequest lsmDBRq = req.getContext();
            
             // send a response to the application, if available
            if (lsmDBRq!=null)
            	lsmDBRq.getListener().insertFinished(lsmDBRq);
             // send a response to the client, if available
            if (pinkyRq!=null)
            	sendResponse(pinkyRq);
        }
        
        remove(rq);
        Logging.logMessage(Logging.LEVEL_INFO, this, "Request finished! \nRequest-details: "+rq.toString());
    }
    
    /**
     * <p>Notifies available listeners for the failure of the given {@link Request} and removes it from the queues.</p>
     * 
     * @param <T>
     * @param rq
     * @param reason
     * @throws ReplicationException if the {@link Request} could not be removed properly. 
     */
    <T> void failed(Status<T> rq,String reason) throws ReplicationException {
        if (rq.getValue() instanceof Request) {
            Request req = (Request) rq.getValue();
            LSMDBRequest lsmDBRq = req.getContext();
            PinkyRequest pinkyRq = req.getOriginal();
            
             // send a response to the application, if available
            if (lsmDBRq!=null)
                lsmDBRq.getListener().requestFailed(lsmDBRq, 
                            new BabuDBException(ErrorCode.REPLICATION_FAILURE,reason));            
             // send a response to the client, if available
            if (pinkyRq!=null){
            	if (!pinkyRq.responseSet)
            		pinkyRq.setResponse(HTTPUtils.SC_SERVER_ERROR, reason);
            	
            	sendResponse(pinkyRq);
            }
            
             // send a error message to the administrator, if no other instance is available
            if (pinkyRq==null && lsmDBRq == null) {
            	Logging.logMessage(Logging.LEVEL_ERROR, this, reason);
            	frontEnd.dbInterface.replication_runtime_failure(reason);
            }
        }
        
        remove(rq);
        Logging.logMessage(Logging.LEVEL_WARN, this, "Request failed! Reason: "+reason+"\nRequest-details: "+rq.toString());
    }
    
    /**
     * <p>Reinserts a request into it's queue due reordering purpose.</p>
     * 
     * @param <T>
     * @param rq
     * @throws ReplicationException if an error occurs.
     */
    @SuppressWarnings("unchecked")
	<T> void statusChanged(Status<T> rq) throws ReplicationException {
        Logging.logMessage(Logging.LEVEL_TRACE, this, "Reordering: "+rq.toString());
        if (rq.getValue() instanceof Request) {
            if (!pending.remove((Status<Request>) rq)) throw new ReplicationException("Request was not in the queue! "+rq.toString());
            pending.add((Status<Request>) rq);
        } else if (rq.getValue() instanceof LSN) {
            if (!missing.remove((Status<LSN>) rq)) throw new ReplicationException("Request was not in the queue! "+rq.toString());
            missing.add((Status<LSN>) rq);
        } else if (rq.getValue() instanceof Chunk) {
            if (!missingChunks.remove((Status<Chunk>) rq)) throw new ReplicationException("Request was not in the queue! "+rq.toString());
            missingChunks.add((Status<Chunk>) rq);
        } else
            throw new ReplicationException("Malformed request: "+rq.toString());
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
     * <p>This is a send and ignore call.</p>
     * 
     * @param request - for retrying purpose.
     * @throws ReplicationException - if an error occurs.
     */
    void sendACK(Request request) throws ReplicationException{
        try {
            SpeedyRequest sReq = new SpeedyRequest(HTTPUtils.POST_TOKEN,Token.ACK.toString(),null,null,ReusableBuffer.wrap(request.getLSN().toString().getBytes()),DATA_TYPE.BINARY);
            speedy.sendRequest(sReq, frontEnd.master);
        } catch (Exception e) {
            throw new ReplicationException("ACK ("+request.getLSN().toString()+") could not be send to master: '"+frontEnd.master.toString()+"', because: "+e.getLocalizedMessage());   
        } 
    }
    
    /**
     * <p>Changes the replication mechanism behavior to master.</p>
     */
    void toMaster(){
        this.setName("Master");
        missing = null;
        missingChunks = null;
        
        slavesStatus.clear();
    }
    
    /**
     * <p>Changes the replication mechanism behavior to slave.</p>
     */
    void toSlave(){
        this.setName("Slave");
        if (missing == null)
        	missing = new PriorityBlockingQueue<Status<LSN>>(100,new PriorityQueueComparator<LSN>());
        if (missingChunks == null)
        	missingChunks = new PriorityBlockingQueue<Status<Chunk>>(10,new PriorityQueueComparator<Chunk>());
        
        slavesStatus.clear();
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

        running = false;
        if (pinky!=null && pinky.isAlive()) pinky.shutdown();
        if (speedy!=null && speedy.isAlive()) speedy.shutdown();  
        notifyCrashed(new ReplicationException(message));
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
    private boolean hasHighPriority(Status<Request> rq) {
        assert (rq!=null) : "Empty request can not have high priority. Use constant LOW_PRIORITY_REQUEST instead.";
        synchronized (nextExpected) {
        	return PENDING_QUEUE_COMPARATOR.compare(rq, nextExpected.get()) <= 0;
		}      
    }
    
    /**
     * @param rq - wraps Status around the request and sets it as nextExpected.
     */
    private void setNextExpected(Request rq) {
        setNextExpected(new Status<Request>(rq,this));
    }
    
    /**
     * @param rq - set as nextExpected.
     */
    private void setNextExpected(Status<Request> rq) {
    	synchronized (nextExpected) {
			nextExpected.set(rq);
		}
    	Logging.logMessage(Logging.LEVEL_TRACE, this, "The next-expected request is '"+rq.toString()+"'");
    }
    
    /**
     * <p>Sets the nextExpected to the lowest priority.</p>
     */
    private void resetNextExpected() {
    	setNextExpected(LOWEST_PRIORITY_REQUEST);
    }
}
