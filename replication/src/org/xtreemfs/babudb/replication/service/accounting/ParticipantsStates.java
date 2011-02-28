/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.accounting;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.PriorityBlockingQueue;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.HeartbeatThread;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;
import org.xtreemfs.babudb.replication.transmission.ClientFactory;
import org.xtreemfs.babudb.replication.transmission.PBRPCClientAdapter;
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

public class ParticipantsStates implements ParticipantsOverview, StatesManipulation {  
    
    /**
     * State of a registered participant.
     * 
     * @since 05/03/2009
     * @author flangner
     */
    private static final class State implements Comparable<State>{
        long lastUpdate;
        boolean dead;
        LSN lastAcknowledged;
        int openRequests;
        final PBRPCClientAdapter client;
        
        /**
         * initial state
         * 
         * @param client
         */
        State(PBRPCClientAdapter client) {
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
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof State && client.equals(((State) obj).client)) {
                return true;
            }
            return false;
        }
        
        /* (non-Javadoc)
         * @see java.lang.Comparable#compareTo(java.lang.Object)
         */
        @Override
        public int compareTo(State o) {
            return lastAcknowledged.compareTo(o.lastAcknowledged);
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
    private final static int                    MAX_OPEN_REQUESTS_PRO_SERVER = 
        20;
    
    /**
     * Determines how long the master should wait for busy slaves to become
     * available again, before it refuses a replication request.  
     */
    private final static long                   DELAY_TILL_REFUSE = 
        HeartbeatThread.MAX_DELAY_BETWEEN_HEARTBEATS;
    
    private final Map<String, State>       stateTable = new HashMap<String, State>(); 

    private final PriorityBlockingQueue<LatestLSNUpdateListener> listeners = 
        new PriorityBlockingQueue<LatestLSNUpdateListener>();
        
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
     * @throws UnknownParticipantException if the address of at least one participant could not have 
     *                                     been resolved.
     */
    public ParticipantsStates(int syncN, Set<InetSocketAddress> participants, 
            ClientFactory clientFactory) throws UnknownParticipantException {
        
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
            for (InetSocketAddress participant : participants) {
                stateTable.put(getUID(participant), 
                               new State(clientFactory.getClient(participant)));
            }
            
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
     * @throws UnknownHostException if the address of at least one participant could not have been
     *                              resolved.
     */
    public List<SlaveClient> getAvailableParticipants() throws 
            NotEnoughAvailableParticipantsException, InterruptedException {
        
        List<SlaveClient> result = new LinkedList<SlaveClient>();
        
        synchronized (stateTable) {
            
            // wait until enough slaves are available, if they are ...
            while (availableSlaves < getSyncN() && 
                  !((participantsCount - deadSlaves) < getSyncN()))
                stateTable.wait(DELAY_TILL_REFUSE);
            
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
                        } else if ( s.openRequests < MAX_OPEN_REQUESTS_PRO_SERVER ) {
                            
                            s.openRequests++;
                            if ( s.openRequests == MAX_OPEN_REQUESTS_PRO_SERVER ) 
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
                    State s;
                    try {
                        s = stateTable.get(getUID(c.getDefaultServerAddress()));
                        if (s.openRequests == MAX_OPEN_REQUESTS_PRO_SERVER) 
                            availableSlaves++;
                        
                        s.openRequests--;
                    } catch (UnknownParticipantException e) {
                        Logging.logMessage(Logging.LEVEL_ALERT, this, "An open request could not " +
                        		"have been withdrawn, because it's address could not have" +
                        		" been resolved anymore!");
                        Logging.logError(Logging.LEVEL_ERROR, this, e);
                    }
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
                if (latestCommon.compareTo(listener.lsn) >= 0) {
                    listener.upToDate();
                    return;
                }
            }
            listeners.add(listener);
        }
    }
    
/*
 * Overridden methods
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.StatesManipulation#update(
     *          java.net.InetSocketAddress, org.xtreemfs.babudb.lsmdb.LSN, long)
     */
    @Override
    public void update(InetSocketAddress participant, LSN acknowledgedLSN, long receiveTime) 
            throws UnknownParticipantException {
                
        Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                "participant %s acknowledged %s", 
                participant.toString(), acknowledgedLSN.toString());
        
        synchronized (stateTable) {
            
            // the latest common LSN is >= the acknowledged one, just update the 
            // participant
            final State old = stateTable.get(getUID(participant));
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
                
                // count the number of LSN greater-than-or-equal than the acknowledged 
                // to get the latest common
                if (old.lastAcknowledged.compareTo(acknowledgedLSN) < 0) {
                    old.lastAcknowledged = acknowledgedLSN;
                    
                    int count = 0;
                    for (State s : stateTable.values()) {
                        
                        if (!s.dead && s.lastAcknowledged.compareTo(acknowledgedLSN) >= 0) {
                            
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
     * @see org.xtreemfs.babudb.replication.service.accounting.
     *          StatesManipulation#markAsDead(
     *               org.xtreemfs.babudb.replication.transmission.client.Client)
     */
    @Override
    public void markAsDead(SlaveClient slave) {
        
        synchronized (stateTable) {
            State s;
            try {
                s = stateTable.get(getUID(slave.getDefaultServerAddress()));
            } catch (UnknownParticipantException e) {
                Logging.logMessage(Logging.LEVEL_ALERT, this, "Request could not have been marked" +
                " as failed, because it's address could not have been resolved anymore!");
                Logging.logError(Logging.LEVEL_ERROR, this, e);
                return;
            }
            s.openRequests = 0;
            
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
     * @see org.xtreemfs.babudb.replication.service.accounting.
     *          StatesManipulation#requestFinished(
     *               org.xtreemfs.babudb.replication.transmission.client.Client)
     */
    @Override
    public void requestFinished(SlaveClient slave) {
        
        synchronized (stateTable) {
            State s;
            try {
                s = stateTable.get(getUID(slave.getDefaultServerAddress()));
            } catch (UnknownParticipantException e) {
                Logging.logMessage(Logging.LEVEL_ALERT, this, "Request could not have been marked" +
                        " as finished, because it's address could not have been resolved anymore!");
                Logging.logError(Logging.LEVEL_ERROR, this, e);
                return;
            }
            if (s.openRequests > 0) s.openRequests--;
            
            // the number of open requests for this slave has fallen below the
            // threshold of MAX_OPEN_REQUESTS_PER_SLAVE
            if (s.openRequests == (MAX_OPEN_REQUESTS_PRO_SERVER - 1)) {
                
                availableSlaves++;
                stateTable.notify();
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.
     *          ParticipantsOverview#getConditionClients()
     */
    @Override
    public List<ConditionClient> getConditionClients() {
        
        List<ConditionClient> result = new ArrayList<ConditionClient>();
        List<State> states = new ArrayList<State>(stateTable.values());
        Collections.sort(states);
        Collections.reverse(states);
        for (State s : states) {
            result.add(s.client);
        }
        return result;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview#getByAddress(
     *          java.net.InetSocketAddress)
     */
    @Override
    public ConditionClient getByAddress(InetSocketAddress address) throws UnknownParticipantException {
        return stateTable.get(getUID(address)).client;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        
        synchronized (stateTable) {
            
            String result = "ParticipantsStates: participants=" + 
                            participantsCount + " - available=" + 
                            availableSlaves + "|dead=" + deadSlaves + "\n";
        
            for (Entry<String, State> e : stateTable.entrySet()) {
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
        if (listener != null) {
            listeners.add(listener);
        }
    }
    
    /**
     * <p>
     * Resets the state of any available participants and removes all available listeners from the 
     * queue. The listeners will be notified that the operation they are listening for has failed.
     * </p>
     */
    public void reset() {
        
        // deal with the listeners
        Set<LatestLSNUpdateListener> lSet = Collections.emptySet();
        listeners.drainTo(lSet);
        for (LatestLSNUpdateListener l : lSet) {
            l.failed();
        }
        
        // deal with states
        synchronized (stateTable) {
            
            for (State s : stateTable.values()) {
                s.reset();
            }
        }
    }
    
    /**
     * @param address
     * @return a unique ID from address usable as hashable key.
     * @throws UnknownParticipantException if the address of at least one participant could not have 
     *                                     been resolved.
     */
    private String getUID(InetSocketAddress address) throws UnknownParticipantException {
        
        assert (address != null);
        
        String hostAddress;
        if (address.getAddress() == null) {
            try {
                hostAddress = InetAddress.getByName(address.getHostName()).getHostAddress();
            } catch (UnknownHostException uh) {
                throw new UnknownParticipantException("the address of '" + address.toString() + 
                        "' could not have been determined.");
            }
        } else {
            hostAddress = address.getAddress().getHostAddress();
        }
        
        return hostAddress + ":" + address.getPort();
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
    public static class NotEnoughAvailableParticipantsException extends Exception {
        
        private static final long serialVersionUID = 5521213821006794885L;     
        
        public NotEnoughAvailableParticipantsException(String string) {
            super(string);
        }
    }
}