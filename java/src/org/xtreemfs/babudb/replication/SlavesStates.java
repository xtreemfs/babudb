/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

import org.xtreemfs.babudb.clients.SlaveClient;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.stages.HeartbeatThread;
import org.xtreemfs.include.common.TimeSync;
import org.xtreemfs.include.foundation.oncrpc.client.RPCNIOSocketClient;

/**
 * <p>Holds registered slaves by their {@link InetAddress} and their {@link State}.</p>
 * <p>Also includes a client for every registered slave.</p>
 * <p>Operations are thread-save.</p>
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class SlavesStates {
    
    // if a slave does not send a heartBeat in twice of the maximum delay between two heartBeats,
    // than it is definitively to slow and is dead, or will be dead soon.
    public final static long DELAY_TILL_DEAD = 2 * HeartbeatThread.MAX_DELAY_BETWEEN_HEARTBEATS;
    public final static int MAX_OPEN_REQUESTS_PRO_SLAVE = 10;
    
    private HashMap<InetAddress, State> stateTable = new HashMap<InetAddress, State>(); 

    private volatile LSN latestCommon;
    
    private final int syncN;
    
    private final PriorityBlockingQueue<LatestLSNUpdateListener> listeners = new PriorityBlockingQueue<LatestLSNUpdateListener>();
    
    /**
     * Sets the stateTable up. 
     * 
     * @param syncN
     * @param slaves - to register.
     */
    public SlavesStates(int syncN,List<InetSocketAddress> slaves, RPCNIOSocketClient client) {
        assert(slaves!=null);
        
        latestCommon = new LSN(0,0L);
        this.syncN = syncN;
        
        for (InetSocketAddress slave : slaves) 
            stateTable.put(slave.getAddress(), 
                    new State(new SlaveClient(client,slave)));
    }
    
    /**
     * @param slave
     * @param acknowledgedLSN
     * @param receiveTime
     * @return the latestCommon {@link LSN}.
     * 
     * @throws UnknownParticipantException if the slave is not registered.
     */
    public LSN update(InetAddress slave, LSN acknowledgedLSN, long receiveTime) throws UnknownParticipantException{
        synchronized (stateTable) {
            // the latest common LSN is >= the acknowledged one, just update the slave
            State old;
            if ((old = stateTable.get(slave))!=null) {            
                // count the number of LSN ge than the acknowledged to get the latest common
                if (old.lastAcknowledged.compareTo(acknowledgedLSN)<0) {
                    old.lastAcknowledged = acknowledgedLSN;
                    old.lastUpdate = receiveTime;
                    old.dead = false;
                    
                    int count = 0;
                    for (State s : stateTable.values()){
                        if (!s.dead && s.lastAcknowledged.compareTo(acknowledgedLSN)>=0){
                            count++;
                            if (count>=syncN) {
                                this.latestCommon = acknowledgedLSN;
                                notifyListeners();
                                break;
                            }
                        }
                    }
                }
            } else 
                throw new UnknownParticipantException("'"+slave.toString()+"' is not registered at this master. Request received: "+receiveTime);
            
            return latestCommon;
        }
    }
    
    /**
     * @return the latest LSN acknowledged by at least syncN slaves.
     */
    public LSN getLatestCommon() {
        return latestCommon;
    }
    
    /**
     * <p>Use this if you want to send requests to the slaves.
     * The open-request-counter of the returned slaves will be incremented.</p>
     * 
     * @return a list of available slaves.
     * @throws NotEnoughAvailableSlavesException
     */
    public List<SlaveClient> getAvailableSlaves() throws NotEnoughAvailableSlavesException {   
        List<SlaveClient> result = new LinkedList<SlaveClient>();
        long time = TimeSync.getLocalSystemTime();
        
        synchronized (stateTable) {
            for (InetAddress slave : stateTable.keySet()){
                State slaveState = stateTable.get(slave);
                if (!slaveState.dead){
                    if (slaveState.lastUpdate!=0L && time>(slaveState.lastUpdate+DELAY_TILL_DEAD))
                        slaveState.dead = true;
                    else if (slaveState.openRequests<MAX_OPEN_REQUESTS_PRO_SLAVE) {
                        slaveState.openRequests++;
                        result.add(slaveState.client);
                    }
                }
            }   
        }
        if (result.size()<syncN) throw new NotEnoughAvailableSlavesException("There were not enough slaves to perform the request.");
            
        return result;
    }

    /**
     * Registers a listener to notify, if the latest common {@link LSN} has changed.
     * Listeners will be registered in natural order of their LSNs.
     * 
     * @param listener
     */
    public void subscribeListener(LatestLSNUpdateListener listener) {
        assert(listener.lsn!=null) : "LSN has not been set!";
        if (syncN == 0){
            latestCommon = listener.lsn;
            listener.upToDate();
        } else {
            listeners.add(listener);
        }
    }
    
    /**
     * Marks a slave manually as dead and decrements the open requests for the slave.
     * 
     * @param slave
     */
    public void markAsDead(SlaveClient slave) {
        synchronized (stateTable) {
            State s = stateTable.get(slave.getDefaultServerAddress().getAddress());
            s.dead = true;
            s.openRequests--;
        }
    }
    
    /**
     * Decrements the open requests for the slave.
     * 
     * @param slave
     */
    public void requestFinished(SlaveClient slave) {
        synchronized (stateTable) {
            stateTable.get(slave.getDefaultServerAddress().getAddress()).openRequests--;
        }
    }
    
    /**
     * Notifies all registered listeners about the new latest common LSN.
     */
    private void notifyListeners(){
        LatestLSNUpdateListener listener = listeners.poll();
        while (listener!=null && listener.lsn.compareTo(latestCommon)<=0) {
            listener.upToDate();
            listener = listeners.poll();
        }
        if (listener!=null) listeners.add(listener);
    }
    
/*
 * Exceptions    
 */
    
    public static class UnknownParticipantException extends Exception {
        private static final long serialVersionUID = -2709960657015326930L; 
        
        public UnknownParticipantException(String string) {
            super(string);
        }
    }
    
    public static class NotEnoughAvailableSlavesException extends Exception {
        private static final long serialVersionUID = 5521213821006794885L;     
        
        public NotEnoughAvailableSlavesException(String string) {
            super(string);
        }
    }
    
    /**
     * State of a registered slave.
     * 
     * @since 05/03/2009
     * @author flangner
     */
    private class State {
        long lastUpdate = 0L;
        boolean dead = false;
        LSN lastAcknowledged = new LSN(0,0L);
        int openRequests = 0;
        final SlaveClient client;
        
        /**
         * initial state
         * 
         * @param client
         */
        State(SlaveClient client) {
            this.client = client;
        }
    }
}