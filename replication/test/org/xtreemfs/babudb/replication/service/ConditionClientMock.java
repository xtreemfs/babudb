package org.xtreemfs.babudb.replication.service;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Timestamp;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;

public class ConditionClientMock implements ConditionClient {

    public ConditionClientMock(InetSocketAddress addr) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public ClientResponseFuture<Object, ErrorCodeResponse> flease(
            FleaseMessage message) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClientResponseFuture<Object, ErrorCodeResponse> heartbeat(LSN lsn,
            int localPort) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClientResponseFuture<LSN, org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN> state() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClientResponseFuture<Long, Timestamp> time() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClientResponseFuture<LSN, org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN> volatileState() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InetSocketAddress getDefaultServerAddress() {
        // TODO Auto-generated method stub
        return null;
    }

}
