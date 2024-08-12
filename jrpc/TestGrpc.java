package jrpc;

import java.util.HashMap;
import java.util.Map;

public class TestGrpc {
    public static void main(String[] args) throws Exception {
        ProtocolBuffers pb = new ProtocolBuffers("test.proto");

        Map<String, GrpcHandler> handlers = new HashMap<>();
        handlers.put("/UserService/GetUser", (request) -> {
            System.out.println("Received: " + request);
            MessageObject response = new MessageObject(pb, "UserResponse");
           
            response.setField("id", request.getField("id"));
            response.setField("name", "John Doe");

            Map<String, String> userRoles = new HashMap<>();
            userRoles.put("admin", "true");
            userRoles.put("editor", "true");
            response.setField("properties", userRoles);

            return response;
        });

        new GrpcServer(pb, handlers).start();
    }
}
