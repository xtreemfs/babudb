/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.accounting;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.HeartbeatThread;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;
import org.xtreemfs.babudb.replication.transmission.ClientFactory;
import org.xtreemfs.babudb.replication.transmission.client.Client;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.logging.Logging;

/**
 * <p>
 * Accounting of the informations about any participating client necessary to
 * use the replication for BabuDB. Holds registered participants by their 
 * {@link InetAddress} and their {@link State}. Also includes a client for 
 * every registered participant. All methods are thread-save.
 * </p>
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class ParticipantsStates implements ParticipantsOverview, 
    StatesManipulation, ParticipantsVerification {  
    
    /**
     * State of a registered participant.
     * 
     * @since 05/03/2009
     * @author flangner
     */
    private static final class State {
        long lastUpdate;
        boolean dead;
        LSN lastAcknowledged;
        int openRequests;
        final Client client;
        
        /**
         * initial state
         * 
         * @param client
         */
        State(Client client) {
            this.client = client;
            reset();
        }
        
        /**
         * resets this state to the initial state
         */
        void reset() {
            lastUpdate = TimeSync.getGlobalTime();
            dead = false;
            lastAcknowledged = new LSN(0,0L);
            openRequests = 0;
        }
        
        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return ((dead) ? "dead" : "alive") +
                   " since '" + lastUpdate + "' with LSN (" +
                   lastAcknowledged.toString() + ") and '" +
                   openRequests + "' open requests;";
        }
    }
    
    /** 
     * <p>
     * If a participant does not send a heartBeat in twice of the maximum delay 
     * between two heartBeats, than it is definitively to slow and must be dead, 
     * or will be dead soon.
     * </p>
     */ 
    private final static long                   DELAY_TILL_DEAD = 
        2 * HeartbeatThread.MAX_DELAY_BETWEEN_HEARTBEATS;
    
    /** 
     * <p>
     * The number of request a master can send to one participant is limited by 
     * this field to prevent it from killing the slave by a DoS.
     * </p>
     */
    private final static int                    MAX_OPEN_REQUESTS_PER_SLAVE = 
        20;
    
    /**
     * Determines how long the master should wait for busy slaves to become
     * available again, before it refuses a replication request.  
     */
    private final static long                   WAIT_TILL_REFUSE = 
        HeartbeatThread.MAX_DELAY_BETWEEN_HEARTBEATS;
    
    private final Map<InetAddress, State>       stateTable = 
        new HashMap<InetAddress, State>(); 

    private final PriorityBlockingQueue<LatestLSNUpdateListener> listeners = 
        new PriorityBlockingQueue<LatestLSNUpdateListener>();
    
    private final AtomicReference<MasterClient> masterClient = 
        new AtomicReference<MasterClient>(null);
    
    private final int                           syncN;
    
    private final int                           participantsCount;
    
    private int                                 availableSlaves;
    
    private int                                 deadSlaves;
    
    private volatile LSN                        latestCommon;
    
    /**
     * Sets the stateTable up. 
     * 
     * @param syncN
     * @param localAddress
     * @param participants - to register.
     */
    public ParticipantsStates(int syncN, Set<InetSocketAddress> participants, 
            ClientFactory clientFactory) {
        
        assert(participants!=null);
        
        this.latestCommon = new LSN(0,0L);
        this.syncN = syncN;
        this.participantsCount = this.availableSlaves = participants.size();
        this.deadSlaves = 0;
        
        /*
         * The initial set of participants accounted by the table is must
         * not be changed during the runtime!
         */
        synchronized (stateTable) {
            for (InetSocketAddress participant : participants) 
                stateTable.put(participant.getAddress(), 
                        new State(clientFactory.getClient(participant)));
            
            Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                    "Initial configuration:\n%s", toString());
        }
    }
    
    /**
     * @return the syncN
     */
    public int getSyncN() {
        return syncN;
    }
    
    /**
     * @return the latest LSN acknowledged by at least syncN participants.
     */
    public LSN getLatestCommon() {
        return latestCommon;
    }
    
    /**
     * <p>
     * Use this if you want to send requests to the participants. The 
     * open-request-counter of the returned participants will be incremented.
     * </p>
     * <p>
     * Flow-control: If there are too many busy participants, to get more or 
     * equal syncN slaves, this operation blocks until enough slaves are 
     * available.
     * </p>
     * 
     * @return a list of available participants.
     * @throws NotEnoughAvailableParticipantsException
     * @throws InterruptedException 
     */
    public List<SlaveClient> getAvailableParticipants() throws 
            NotEnoughAvailableParticipantsException, InterruptedException{
        
        List<SlaveClient> result = new LinkedList<SlaveClient>();
        
        synchronized (stateTable) {
            
            // wait until enough slaves are available, if they are ...
            while (availableSlaves < getSyncN() && 
                  !((participantsCount - deadSlaves) < getSyncN()))
                stateTable.wait(WAIT_TILL_REFUSE);
            
            long time = TimeSync.getGlobalTime();
            
            // get all available slaves, if they are enough ...
            if (!((participantsCount - deadSlaves) < getSyncN())) {
                for (final State s : stateTable.values()){
                    if (!s.dead){
                        if ( time > ( s.lastUpdate + DELAY_TILL_DEAD ) ) {
                            
                            Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                                    "%s will be marked as dead!\n%s",
                                    s.client.getDefaultServerAddress().toString(),
                                    toString());
                            s.dead = true;
                            availableSlaves--;
                        } else if ( s.openRequests < 
                                    MAX_OPEN_REQUESTS_PER_SLAVE ) {
                            s.openRequests++;
                            if ( s.openRequests == MAX_OPEN_REQUESTS_PER_SLAVE ) 
                                availableSlaves--;
                            
                            result.add(s.client);
                        } 
                    }
                }   
            }
            
            // throw an exception, if they are not.
            if (result.size() < getSyncN()) {
                
                // recycle the slaves from the result, before throwing an exception
                for (SlaveClient c : result) {
                    State s = stateTable.get(
                            c.getDefaultServerAddress().getAddress());
                    if (s.openRequests == MAX_OPEN_REQUESTS_PER_SLAVE) 
                        availableSlaves++;
                    
                    s.openRequests--;
                }
                
                throw new NotEnoughAvailableParticipantsException(        
                    "With only '"+result.size()+"' are there not enough " +
                    "slaves to perform the request.");
            }
        }
        return result;
    }

    /**
     * Registers a listener to notify, if the latest common {@link LSN} has 
     * changed. Listeners will be registered in natural order of their LSNs.
     * 
     * @param listener
     */
    public void subscribeListener(LatestLSNUpdateListener listener) {
        
        // fully asynchronous mode
        if (getSyncN() == 0){
            latestCommon = listener.lsn;
            listener.upToDate();
        // N-sync-mode
        } else {
            synchronized (stateTable) {
                if (latestCommon.compareTo(listener.lsn)>=0) {
                    listener.upToDate();
                    return;
                }
            }
            listeners.add(listener);
        }
    }
    
    /**
     * <p>
     * Removes all available listeners from the queue. The listeners will be 
     * notified that the operation they are listening for has failed.
     * </p>
     */
    public void clearListeners(){
        Set<LatestLSNUpdateListener> lSet = 
            new HashSet<LatestLSNUpdateListener>();
        
        listeners.drainTo(lSet);
        for (LatestLSNUpdateListener l : lSet) l.failed();
    }
        
    /**
     * <p>
     * Sets a new master, valid for all replication components. Resets the 
     * states of all participants.
     * </p>
     * 
     * @param address
     */
    public void setMaster(InetAddress address) {
        this.masterClient.set(this.stateTable.get(address).client);
        this.reset();
    }
    
    /**
     * @return the master client currently registered by the replication 
     *         mechanism. May be null.
     */
    public MasterClient getMaster() {
        return masterClient.get();
    }
    
/*
 * Methods implementing the interfaces
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.StatesManipulation#update(java.net.SocketAddress, org.xtreemfs.babudb.lsmdb.LSN, long)
     */
    public void update(SocketAddress participant, LSN acknowledgedLSN, 
            long receiveTime) throws UnknownParticipantException{
        
        assert (participant instanceof InetSocketAddress);
        InetAddress host = ((InetSocketAddress) participant).getAddress();
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                "participant %s acknowledged %s", 
                participant.toString(), acknowledgedLSN.toString());
        
        synchronized (stateTable) {
            // the latest common LSN is >= the acknowledged one, just update the 
            // participant
            final State old = stateTable.get(host);
            if (old != null) { 
                // got a prove of life
                old.lastUpdate = receiveTime;
                if (old.dead) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                            "%s will be marked as alive!\n%s", 
                            participant.toString(), toString());
                    deadSlaves--;
                    availableSlaves++;
                    old.dead = false;
                }
                
                // count the number of LSN ge than the acknowledged 
                // to get the latest common
                if (old.lastAcknowledged.compareTo(acknowledgedLSN)<0) {
                    old.lastAcknowledged = acknowledgedLSN;
                    
                    int count = 0;
                    for (State s : stateTable.values()) {
                        if (!s.dead && s.lastAcknowledged
                                .compareTo(acknowledgedLSN) >= 0) {
                            
                            count++;
                            if (count >= getSyncN()) {
                                this.latestCommon = acknowledgedLSN;
                                notifyListeners();
                                break;
                            }
                        }
                    }
                }
            } else {
                Logging.logMessage(Logging.LEVEL_ERROR, this, "'%s' is not" +
                        " registered at this master. Request received: %d", 
                        participant.toString(), receiveTime);
                
                throw new UnknownParticipantException("'" + participant.toString() + 
                        "' is not registered at this master. " +
                        "Request received: " + receiveTime);        
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.StatesManipulation#markAsDead(org.xtreemfs.babudb.replication.transmission.client.Client)
     */
    public void markAsDead(SlaveClient slave) {
        synchronized (stateTable) {
            final State s = 
                stateTable.get(slave.getDefaultServerAddress().getAddress());
            s.openRequests--;
            
            // the slave has not been marked as dead jet
            if (!s.dead) {
                Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                        "%s will be marked as dead!\n%s",
                        slave.getDefaultServerAddress().toString(), 
                        toString());
                
                s.dead = true;
                deadSlaves++;
                availableSlaves--;
                stateTable.notify();
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.StatesManipulation#requestFinished(org.xtreemfs.babudb.replication.transmission.client.Client)
     */
    public void requestFinished(SlaveClient slave) {
        synchronized (stateTable) {
            final State s = 
                stateTable.get(slave.getDefaultServerAddress().getAddress());
            s.openRequests--;
            
            // the number of open requests for this slave has fallen below the
            // threshold of MAX_OPEN_REQUESTS_PER_SLAVE
            if (s.openRequests == (MAX_OPEN_REQUESTS_PER_SLAVE-1)) {
                availableSlaves++;
                stateTable.notify();
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview#getConditionClients()
     */
    public List<ConditionClient> getConditionClients() {
        List<ConditionClient> result = new Vector<ConditionClient>();
        for (State s : stateTable.values()) result.add(s.client);
        return result;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview#getByAddress(java.net.InetAddress)
     */
    public ConditionClient getByAddress(InetAddress address) {
        return stateTable.get(address).client;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.ParticipantsVerification#isMaster(java.net.SocketAddress)
     */
    @Override
    public boolean isMaster(SocketAddress address) {
        InetAddress masterAddress = 
            getMaster().getDefaultServerAddress().getAddress();
        
        return (address instanceof InetSocketAddress) &&
               ((InetSocketAddress) address).getAddress().equals(masterAddress);

    }
    
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.ParticipantsVerification#isRegistered(java.net.SocketAddress)
     */
    public boolean isRegistered(SocketAddress address) {
        if (address instanceof InetSocketAddress) {
            return stateTable.containsKey(
                    ((InetSocketAddress) address).getAddress());
        }
        
        Logging.logMessage(Logging.LEVEL_ERROR, this, 
                "Access-rights for client '%s' could not be validated.", 
                address.toString());
        
        return false;
    }
     
/*
 * Overridden methods    
 */
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        synchronized (stateTable) {
            String result = "ParticipantsStates: participants=" + 
                            participantsCount + " - available=" + 
                            availableSlaves + "|dead=" + deadSlaves + "\n";
        
            for (Entry<InetAddress, State> e : stateTable.entrySet()) {
                result += e.getKey().toString() + ": " + 
                          e.getValue().toString() + "\n";
            }  
            
            return result;
        }
    }
    
/*
 * Private methods
 */
    
    /**
     * Notifies all registered listeners about the new latest common LSN.
     */
    private void notifyListeners(){
        LatestLSNUpdateListener listener = listeners.poll();
        while (listener != null && listener.lsn.compareTo(latestCommon)<=0) {
            listener.upToDate();
            listener = listeners.poll();
        }
        if (listener != null) listeners.add(listener);
    }
    
    /**
     * <p>Resets the state of any available participants.</p>
     */
    private void reset() {
        synchronized (stateTable) {
            for (State s : stateTable.values()) s.reset();
        }
    }
    
/*
 * Exceptions    
 */
    
    /**
     * @author flangner
     * @since 05/03/2009
     */
    public static class UnknownParticipantException extends Exception {
        private static final long serialVersionUID = -2709960657015326930L; 
        
        public UnknownParticipantException(String string) {
            super(string);
        }
    }
    
    /**
     * @author flangner
     * @since 05/03/2009
     */
    public static class NotEnoughAvailableParticipantsException 
        extends Exception {
        private static final long serialVersionUID = 5521213821006794885L;     
        
        public NotEnoughAvailableParticipantsException(String string) {
            super(string);
        }
    }
}