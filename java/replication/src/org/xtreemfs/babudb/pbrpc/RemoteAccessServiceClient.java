//automatically generated from replication.proto at Fri Jan 28 12:07:07 CET 2011
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

    public RPCResponse<GlobalTypes.DatabaseNames> getDatabaseNames(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, Common.emptyRequest input) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.DatabaseNames> response = new RPCResponse<GlobalTypes.DatabaseNames>(GlobalTypes.DatabaseNames.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 10001, 2, input, null, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.DatabaseNames> getDatabaseNames(InetSocketAddress server, Auth authHeader, UserCredentials userCreds) throws IOException {
         
         return getDatabaseNames(server, authHeader, userCreds,null);
    }

    public RPCResponse<GlobalTypes.ErrorCodeResponse> lookup(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.Lookup input, ReusableBuffer data) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.ErrorCodeResponse> response = new RPCResponse<GlobalTypes.ErrorCodeResponse>(GlobalTypes.ErrorCodeResponse.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 10001, 3, input, data, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.ErrorCodeResponse> lookup(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, String database_name, int index_id, ReusableBuffer data) throws IOException {
         final GlobalTypes.Lookup msg = GlobalTypes.Lookup.newBuilder().setDatabaseName(database_name).setIndexId(index_id).build();
         return lookup(server, authHeader, userCreds,msg, data);
    }

    public RPCResponse<GlobalTypes.EntryMap> plookup(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.Lookup input, ReusableBuffer data) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.EntryMap> response = new RPCResponse<GlobalTypes.EntryMap>(GlobalTypes.EntryMap.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 10001, 4, input, data, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.EntryMap> plookup(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, String database_name, int index_id, ReusableBuffer data) throws IOException {
         final GlobalTypes.Lookup msg = GlobalTypes.Lookup.newBuilder().setDatabaseName(database_name).setIndexId(index_id).build();
         return plookup(server, authHeader, userCreds,msg, data);
    }

    public RPCResponse<GlobalTypes.EntryMap> plookupReverse(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.Lookup input, ReusableBuffer data) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.EntryMap> response = new RPCResponse<GlobalTypes.EntryMap>(GlobalTypes.EntryMap.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 10001, 5, input, data, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.EntryMap> plookupReverse(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, String database_name, int index_id, ReusableBuffer data) throws IOException {
         final GlobalTypes.Lookup msg = GlobalTypes.Lookup.newBuilder().setDatabaseName(database_name).setIndexId(index_id).build();
         return plookupReverse(server, authHeader, userCreds,msg, data);
    }

    public RPCResponse<GlobalTypes.EntryMap> rlookup(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.RangeLookup input, ReusableBuffer data) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.EntryMap> response = new RPCResponse<GlobalTypes.EntryMap>(GlobalTypes.EntryMap.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 10001, 6, input, data, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.EntryMap> rlookup(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, String database_name, int index_id, int from_length, ReusableBuffer data) throws IOException {
         final GlobalTypes.RangeLookup msg = GlobalTypes.RangeLookup.newBuilder().setDatabaseName(database_name).setIndexId(index_id).setFromLength(from_length).build();
         return rlookup(server, authHeader, userCreds,msg, data);
    }

    public RPCResponse<GlobalTypes.EntryMap> rlookupReverse(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.RangeLookup input, ReusableBuffer data) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.EntryMap> response = new RPCResponse<GlobalTypes.EntryMap>(GlobalTypes.EntryMap.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 10001, 7, input, data, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.EntryMap> rlookupReverse(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, String database_name, int index_id, int from_length, ReusableBuffer data) throws IOException {
         final GlobalTypes.RangeLookup msg = GlobalTypes.RangeLookup.newBuilder().setDatabaseName(database_name).setIndexId(index_id).setFromLength(from_length).build();
         return rlookupReverse(server, authHeader, userCreds,msg, data);
    }

    public boolean clientIsAlive() {
        return client.isAlive();
    }
}