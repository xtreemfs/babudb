/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.util.Comparator;

import org.xtreemfs.babudb.lsmdb.LSN;

/**
 * <p>Guarantees a specific order in the priority queues of the {@link ReplicationThread}.</p>
 * 
 * @author flangner
 *
 * @param <T>
 */

public class PriorityQueueComparator<T> implements Comparator<Status<T>> {

    /*
     * (non-Javadoc)
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    @Override
    public int compare(Status<T> o1, Status<T> o2) {
        if (o1.getStatus().equals(o2.getStatus())) {
            // make a case differentiation for not comparing apples with oranges     

            // compare requests
            if (o1.getValue() instanceof Request){
                Request rq1 = (Request) o1.getValue();
                Request rq2 = (Request) o2.getValue();
                // lowest priority
                if (rq2==null) return +1;

                /*
                 * Order for the pending queue in ReplicationThread.
                 */  
                Token rq1t = rq1.getToken();
                Token rq2t = rq2.getToken();
                if (rq1t.compareTo(rq2t)==0){
                    switch (rq1t) {
                    // master
                    case ACK:                   return rq2.getLSN().compareTo(rq1.getLSN());
                    case RQ:                    return rq1.getLSN().compareTo(rq2.getLSN());
                    case REPLICA_BROADCAST:     return rq1.getLSN().compareTo(rq2.getLSN());
                    case CHUNK:                 return rq1.getChunkDetails().compareTo(rq2.getChunkDetails());
                    
                    // slave
                    case REPLICA:               return rq1.getLSN().compareTo(rq2.getLSN());
                    case CHUNK_RP:              return rq1.getChunkDetails().compareTo(rq2.getChunkDetails());
                    
                    default:                    return 0;
                    }
                }
                return rq1t.compareTo(rq2t);
            
            // compare missing LSN requests
            } else if (o1.getValue() instanceof LSN) {
                LSN lsn1 = (LSN) o1.getValue();
                LSN lsn2 = (LSN) o2.getValue();
                return lsn1.compareTo(lsn2);
                
            // compare missing Chunk requests    
            } else if (o1.getValue() instanceof Chunk) {
                Chunk chunk1 = (Chunk) o1.getValue();
                Chunk chunk2 = (Chunk) o2.getValue();
                return chunk1.compareTo(chunk2);
                
            // unknown type of request has low priority    
            } else
                return +1;
        }else 
            return o1.getStatus().compareTo(o2.getStatus());
    }
}