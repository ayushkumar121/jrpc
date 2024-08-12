package jrpc;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

public class TestProtocolBuffers {
    public static void main(String[] args) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ProtocolBuffers pb = new ProtocolBuffers("test.proto");

            MessageObject userRequest = new MessageObject(pb, "UserRequest");
            userRequest.setField("id", 123);

            DataOutputStream file = new DataOutputStream(new 
                                 FileOutputStream("request.bat"));
            file.writeByte(0);
            file.writeByte(0);
            file.writeByte(0);
            file.writeByte(0);
            file.writeByte(0);
            userRequest.serialize(file);
            userRequest.serialize(out);

            Utils.printBytes("User Request Bytes: ", out.toByteArray());

            out.reset();

            MessageObject userResponse = new MessageObject(pb, "UserRequest");
            userResponse.setField("id", 123);
            userResponse.setField("name", "John Doe");

            Map<String, String> userProperties = new HashMap<>();
            userProperties.put("ROLE", "admin");

            userResponse.setField("properties", userProperties);
            userResponse.serialize(out);
            Utils.printBytes("User Response Bytes: ", out.toByteArray());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
