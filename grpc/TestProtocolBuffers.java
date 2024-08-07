package grpc;

import java.io.ByteArrayOutputStream;
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
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ProtocolBuffers pb = new ProtocolBuffers("test.proto");

            ProtocolBuffers.MessageObject userRequest = pb.newMessageObject("UserRequest");
            userRequest.setField("id", 123);

            userRequest.serialize(out);
            printBytes("User Request Bytes: ", out.toByteArray());

            out.reset();

            ProtocolBuffers.MessageObject userResponse = pb.newMessageObject("UserResponse");
            userResponse.setField("id", 123);
            userResponse.setField("name", "John Doe");

            Map<String, String> userProperties = new HashMap<>();
            userProperties.put("ROLE", "admin");

            userResponse.setField("properties", userProperties);
            userResponse.serialize(out);
            printBytes("User Response Bytes: ", out.toByteArray());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
