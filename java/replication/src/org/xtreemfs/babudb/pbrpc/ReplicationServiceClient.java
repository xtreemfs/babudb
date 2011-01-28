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

public class ReplicationServiceClient {

    private RPCNIOSocketClient client;
    private InetSocketAddress  defaultServer;

    public ReplicationServiceClient(RPCNIOSocketClient client, InetSocketAddress defaultServer) {
        this.client = client;
        this.defaultServer = defaultServer;
    }

    public RPCResponse<GlobalTypes.LSN> state(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, Common.emptyRequest input) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.LSN> response = new RPCResponse<GlobalTypes.LSN>(GlobalTypes.LSN.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 20001, 1, input, null, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.LSN> state(InetSocketAddress server, Auth authHeader, UserCredentials userCreds) throws IOException {
         
         return state(server, authHeader, userCreds,null);
    }

    public RPCResponse<GlobalTypes.DBFileMetaDatas> load(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.LSN input) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.DBFileMetaDatas> response = new RPCResponse<GlobalTypes.DBFileMetaDatas>(GlobalTypes.DBFileMetaDatas.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 20001, 2, input, null, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.DBFileMetaDatas> load(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, int view_id, long sequence_no) throws IOException {
         final GlobalTypes.LSN msg = GlobalTypes.LSN.newBuilder().setViewId(view_id).setSequenceNo(sequence_no).build();
         return load(server, authHeader, userCreds,msg);
    }

    public RPCResponse<GlobalTypes.ErrorCodeResponse> chunk(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.Chunk input) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.ErrorCodeResponse> response = new RPCResponse<GlobalTypes.ErrorCodeResponse>(GlobalTypes.ErrorCodeResponse.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 20001, 3, input, null, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.ErrorCodeResponse> chunk(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, String file_name, long start, long end) throws IOException {
         final GlobalTypes.Chunk msg = GlobalTypes.Chunk.newBuilder().setFileName(file_name).setStart(start).setEnd(end).build();
         return chunk(server, authHeader, userCreds,msg);
    }

    public RPCResponse<GlobalTypes.ErrorCodeResponse> flease(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.FLease input, ReusableBuffer data) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.ErrorCodeResponse> response = new RPCResponse<GlobalTypes.ErrorCodeResponse>(GlobalTypes.ErrorCodeResponse.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 20001, 4, input, data, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.ErrorCodeResponse> flease(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, String host, int port, ReusableBuffer data) throws IOException {
         final GlobalTypes.FLease msg = GlobalTypes.FLease.newBuilder().setHost(host).setPort(port).build();
         return flease(server, authHeader, userCreds,msg, data);
    }

    public RPCResponse<GlobalTypes.Timestamp> localTime(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, Common.emptyRequest input) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.Timestamp> response = new RPCResponse<GlobalTypes.Timestamp>(GlobalTypes.Timestamp.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 20001, 5, input, null, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.Timestamp> localTime(InetSocketAddress server, Auth authHeader, UserCredentials userCreds) throws IOException {
         
         return localTime(server, authHeader, userCreds,null);
    }

    public RPCResponse<GlobalTypes.LogEntries> replica(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.LSNRange input) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.LogEntries> response = new RPCResponse<GlobalTypes.LogEntries>(GlobalTypes.LogEntries.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 20001, 6, input, null, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.LogEntries> replica(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.LSN start, GlobalTypes.LSN end) throws IOException {
         final GlobalTypes.LSNRange msg = GlobalTypes.LSNRange.newBuilder().setStart(start).setEnd(end).build();
         return replica(server, authHeader, userCreds,msg);
    }

    public RPCResponse<GlobalTypes.ErrorCodeResponse> heartbeat(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.LSN input) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.ErrorCodeResponse> response = new RPCResponse<GlobalTypes.ErrorCodeResponse>(GlobalTypes.ErrorCodeResponse.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 20001, 7, input, null, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.ErrorCodeResponse> heartbeat(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, int view_id, long sequence_no) throws IOException {
         final GlobalTypes.LSN msg = GlobalTypes.LSN.newBuilder().setViewId(view_id).setSequenceNo(sequence_no).build();
         return heartbeat(server, authHeader, userCreds,msg);
    }

    public RPCResponse<GlobalTypes.ErrorCodeResponse> replicate(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, GlobalTypes.LSN input, ReusableBuffer data) throws IOException {
         if (server == null) server = defaultServer;
         if (server == null) throw new IllegalArgumentException("defaultServer must be set in constructor if you want to pass null as server in calls");
         RPCResponse<GlobalTypes.ErrorCodeResponse> response = new RPCResponse<GlobalTypes.ErrorCodeResponse>(GlobalTypes.ErrorCodeResponse.getDefaultInstance());
         client.sendRequest(server, authHeader, userCreds, 20001, 8, input, data, response, false);
         return response;
    }

    public RPCResponse<GlobalTypes.ErrorCodeResponse> replicate(InetSocketAddress server, Auth authHeader, UserCredentials userCreds, int view_id, long sequence_no, ReusableBuffer data) throws IOException {
         final GlobalTypes.LSN msg = GlobalTypes.LSN.newBuilder().setViewId(view_id).setSequenceNo(sequence_no).build();
         return replicate(server, authHeader, userCreds,msg, data);
    }

    public boolean clientIsAlive() {
        return client.isAlive();
    }
}