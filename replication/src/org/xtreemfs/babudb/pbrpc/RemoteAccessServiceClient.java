//automatically generated from replication.proto at Wed Jan 19 14:43:57 CET 2011
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

    public RPCResponse<GlobalTypes.ErrorCodeResponse> makePersistent(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.Type input, ReusableBuffer data) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.ErrorCodeResponse> response = new RPCResponse<GlobalTypes.ErrorCodeResponse>(GlobalTypes.ErrorCodeResponse.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 10001, 1, input, data, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.ErrorCodeResponse> makePersistent(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, int value, ReusableBuffer data) throws IOException {
         final GlobalTypes.Type msg = GlobalTypes.Type.newBuilder().setValue(value).build();
         return makePersistent(server, authHeader, userCreds,msg, data);
    }

    public boolean clientIsAlive() {
        return client.isAlive();
    }
}