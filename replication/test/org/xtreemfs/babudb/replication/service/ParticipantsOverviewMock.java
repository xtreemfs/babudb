package org.xtreemfs.babudb.replication.service;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;

public class ParticipantsOverviewMock implements ParticipantsOverview {

    private final Map<InetSocketAddress, ConditionClient> clients = 
            new HashMap<InetSocketAddress, ConditionClient>();
    
    public ParticipantsOverviewMock(int basicPort, int numOfParticipants) {
        
        for (int i = 0; i < numOfParticipants; i++) {
            InetSocketAddress addr = new InetSocketAddress(basicPort + numOfParticipants);
            
            clients.put(addr, new ConditionClientMock(addr));
        }
    }
    
    @Override
    public ConditionClient getByAddress(InetSocketAddress address) {
        return clients.get(address);
    }

    @Override
    public List<ConditionClient> getConditionClients() {
        return new LinkedList<ConditionClient>(clients.values());
    }
}
