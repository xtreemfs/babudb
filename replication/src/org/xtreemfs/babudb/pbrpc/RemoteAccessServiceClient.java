//automatically generated from replication.proto at Wed Jan 05 14:46:41 CET 2011
//(c) 2011. See LICENSE file for details.

package org.xtreemfs.babudb.pbrpc;

import java.io.IOException;
import java.util.List;
import java.net.InetSocketAddress;
import com.google.protobuf.Message;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.Auth;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.UserCredentials;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;
import org.xtreemfs.foundation.pbrpc.client.RPCResponse;

public class RemoteAccessServiceClient {

    private RPCNIOSocketClient client;
    private InetSocketAddress  defaultServer;

    public RemoteAccessServiceClient(RPCNIOSocketClient client, InetSocketAddress defaultServer) {
        this.client = client;
        this.defaultServer = defaultServer;
    }

    public RPCResponse makePersistent(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, Common.emptyRequest input, ReusableBuffer data) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse response = new RPCResponse(null);
         client.sendRequest(server, authHeader, userCreds, 10001, 1, input, data, response, false);
         return response;
    }

    public RPCResponse makePersistent(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, ReusableBuffer data) throws IOException {
         
         return makePersistent(server, authHeader, userCreds,null, data);
    }

    public boolean clientIsAlive() {
        return client.isAlive();
    }
}