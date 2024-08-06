package grpc;

import java.util.HashMap;
import java.util.Map;

public class TestProtocolBuffers {

    public static void printBytes(String prefix, byte[] bytes) {
        System.out.println(prefix);
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        System.out.println(sb);
    }

    public static void main(String[] args) {
        try {
            ProtocolBuffers pb = new ProtocolBuffers("test.proto");

            ProtocolBuffers.MessageObject userRequest = pb.newMessageObject("UserRequest");
            userRequest.setField("id", 123);
            byte[] userRequestBytes = userRequest.serialize();
            printBytes("User Request Bytes", userRequestBytes);

            ProtocolBuffers.MessageObject userResponse = pb.newMessageObject("UserResponse");
            userResponse.setField("id", 123);
            userResponse.setField("name", "John Doe");

            Map<String, String> userProperties = new HashMap<>();
            userProperties.put("ROLE", "admin");

            userResponse.setField("properties", userProperties);
            byte[] userResponseBytes = userResponse.serialize();
            printBytes("User Response Bytes", userResponseBytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
