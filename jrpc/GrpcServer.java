package jrpc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GrpcServer {
    private static final int PORT = 8080;
    private static final int THREAD_POOL_SIZE = 10;

    private ProtocolBuffers pb;

    public GrpcServer(String protocolBufferFilePath) throws Exception {
        pb = new ProtocolBuffers(protocolBufferFilePath);
        System.err.println("INFO: Server is starting...");
    }

    public void start() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.err.println("INFO: Server is listening on port " + PORT);

            while (true) {
                Socket client = serverSocket.accept();
                executorService.submit(() -> handleClient(client));
            }
        }
    }

    public void handleClient(Socket client) {
        try {
            InputStream in = client.getInputStream();
            OutputStream out = client.getOutputStream();

            byte[] preface = in.readNBytes(HTTP2.EXPECTED_PREFACE.length);
            if (!Arrays.equals(preface, HTTP2.EXPECTED_PREFACE)) {
                System.err.println("ERROR: Invalid preface");
                return;
            }

            System.err.println("INFO: Sending settings frame");

            HTTP2.Frame settingsFrame = new HTTP2.Frame(HTTP2.FRAME_TYPE_SETTINGS, 0x0, 0x0);
            settingsFrame.payload = createSettingsFrameData();
            settingsFrame.serialize(out);

            receiveAcknowledgeFrame(in);

            System.err.println("INFO: Connection established");

            HPack hp = new HPack();
            List<String[]> headers = new ArrayList<>();
            boolean terminate = false;
            int lastStreamId = -1;

            // Map to store streamId and grpc method mapping
            Map<Integer, String> grpcMethodMap = new HashMap<>();
            Map<Integer, ProtocolBuffers.MessageObject> requestMap = new HashMap<>();

            while (!terminate) {
                HTTP2.Frame frame = new HTTP2.Frame(in);
                logFrameDetails(frame);
                
                int streamId = frame.streamId;

                switch (frame.type) {
                    case HTTP2.FRAME_TYPE_HEADERS: {
                        headers.addAll(hp.decode(frame.payload));

                        while ((frame.flag & HTTP2.FLAG_END_HEADERS) == 0) {
                            headers.addAll(hp.decode(new HTTP2.Frame(in).payload));
                        }

                        headers.forEach(header -> {
                            if (header[0].equals(":path")) {
                                grpcMethodMap.put(frame.streamId, header[1]);
                            }
                        });
                    }
                    break;

                    case HTTP2.FRAME_TYPE_DATA: {
                        if (frame.payload.length > 5) {
                            ByteArrayInputStream dataStream = new ByteArrayInputStream(frame.payload);
                            byte[] dataHeader = dataStream.readNBytes(5);
                            if (dataHeader[0] != 0) {
                                sendGoAwayFrame(out, lastStreamId, HTTP2.ERROR_PROTOCOL_ERROR, "Invalid data frame");
                                terminate = true;
                                break;
                            }

                            // TODO: Dispatch the correct method based on the grpcMethodMap
                            ProtocolBuffers.MessageObject request = pb.newMessageObject("UserRequest", dataStream);

                            requestMap.put(frame.streamId, request);
                        }

                        if (frame.flag == HTTP2.FLAG_END_STREAM) {
                            ProtocolBuffers.MessageObject request = requestMap.get(frame.streamId);
                            if (request == null) {
                                sendGoAwayFrame(out, lastStreamId, HTTP2.ERROR_PROTOCOL_ERROR, "Invalid data frame");
                                terminate = true;
                                break;
                            }
                            System.err.println("Object Received: " + request);


                            HTTP2.Frame responseHeaderFrame = new HTTP2.Frame(
                                HTTP2.FRAME_TYPE_HEADERS,
                                HTTP2.FLAG_END_HEADERS, 
                                frame.streamId);
                            responseHeaderFrame.payload = createResponseHeaders(hp);
                            responseHeaderFrame.serialize(out);

                            ProtocolBuffers.MessageObject response = pb.newMessageObject("UserResponse");
                            response.setField("id", request.getField("id"));
                            response.setField("name", "John Doe");

                            Map<String, String> userRoles = new HashMap<>();
                            userRoles.put("admin", "true");
                            userRoles.put("editor", "true");
                            response.setField("properties", userRoles);

                            System.err.println("Object Sent: " + response);

                            ByteArrayOutputStream responseData = new ByteArrayOutputStream();
                            response.serialize(responseData);

                            ByteArrayOutputStream responseStream = new ByteArrayOutputStream();
                            responseStream.write(0); // No compression
                            responseStream.write(Utils.unpack(responseData.size(), 4));
                            responseStream.write(responseData.toByteArray());

                            HTTP2.Frame responseDataFrame = new HTTP2.Frame(
                                HTTP2.FRAME_TYPE_DATA, 
                                0x0,
                                frame.streamId);
                            responseDataFrame.payload = responseStream.toByteArray();
                            responseDataFrame.serialize(out);

                            HTTP2.Frame responseTrailerFrame = new HTTP2.Frame(
                                HTTP2.FRAME_TYPE_HEADERS,
                                HTTP2.FLAG_END_HEADERS | HTTP2.FLAG_END_STREAM, 
                                frame.streamId);
                            responseTrailerFrame.payload = createResponseTrailers(hp);
                            responseTrailerFrame.serialize(out);

                            terminate = true;
                        }
                    }
                    break;
                    
                    case HTTP2.FRAME_TYPE_SETTINGS:
                        if (frame.flag == HTTP2.FLAG_END_STREAM) {
                            System.err.println("INFO: Received settings acknowledgment");
                        } else {
                            sendAcknowledgeFrame(out);
                        }
                        break;
                    case HTTP2.FRAME_TYPE_PING:
                        HTTP2.Frame pingAckFrame = new HTTP2.Frame(
                            HTTP2.FRAME_TYPE_PING, 
                            HTTP2.FLAG_END_STREAM, 0x0);
                        pingAckFrame.payload = frame.payload;
                        pingAckFrame.serialize(out);
                        break;
                    case HTTP2.FRAME_TYPE_GOAWAY:
                        ByteArrayInputStream bais = new ByteArrayInputStream(frame.payload);
                        int lastStream = (int) Utils.pack(bais.readNBytes(4));
                        int errorCode = (int) Utils.pack(bais.readNBytes(4));
                        byte[] debugData = bais.readAllBytes();
                        System.err.println("ERROR: GOAWAY frame received : " + lastStream + " : " + errorCode + " : " + new String(debugData));
                        terminate = true;
                        break;
                    case HTTP2.FRAME_TYPE_WINDOW_UPDATE:
                        break;
                    default:
                        System.err.println("ERROR: Unexpected frame type");
                        return;
                }

                lastStreamId = streamId;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("ERROR: Handling client : " + e.getMessage());
        }

        try {
            client.close();
        } catch (Exception e) {
            System.err.println("ERROR: Unable to close connection : " + e.getMessage());
        }
    }

    private void logFrameDetails(HTTP2.Frame frame) {
        System.err.println("Frame Details:");
        System.err.println("  Length: " + frame.payload.length);
        System.err.println("  Type: " + frame.type);
        System.err.println("  Flags: " + frame.flag);
        System.err.println("  Stream ID: " + frame.streamId);
        System.err.println("  Payload: " + Arrays.toString(frame.payload));
    }

    private void sendGoAwayFrame(OutputStream out, int streamId, int errorCode, String debugData) throws IOException {
        HTTP2.Frame frame = new HTTP2.Frame(HTTP2.FRAME_TYPE_GOAWAY, 0x0, streamId);
        frame.payload = createGoAwayFrameData(streamId, errorCode, debugData);
        frame.serialize(out);
    }

    private byte[] createGoAwayFrameData(int lastStreamId, int errorCode, String debugData) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        try {
            dos.writeInt(lastStreamId);
            dos.writeInt(errorCode);
            dos.write(debugData.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("ERROR: Creating GOAWAY frame data : " + e.getMessage());
        }

        return baos.toByteArray();
}

    private byte[] createSettingsFrameData() throws IOException {
        ByteArrayOutputStream settingsStream = new ByteArrayOutputStream();
        settingsStream.write(Utils.unpack(HTTP2.SETTINGS_HEADER_TABLE_SIZE, 2));
        settingsStream.write(Utils.unpack(4096, 4));
        settingsStream.write(Utils.unpack(HTTP2.SETTINGS_ENABLE_PUSH, 2));
        settingsStream.write(Utils.unpack(0, 4));
        settingsStream.write(Utils.unpack(HTTP2.SETTINGS_MAX_CONCURRENT_STREAMS, 2));
        settingsStream.write(Utils.unpack(1, 4));
        settingsStream.write(Utils.unpack(HTTP2.SETTINGS_INITIAL_WINDOW_SIZE, 2));
        settingsStream.write(Utils.unpack(65535, 4));
        settingsStream.write(Utils.unpack(HTTP2.SETTINGS_MAX_HEADER_LIST_SIZE, 2));
        settingsStream.write(Utils.unpack(8192, 4));
        settingsStream.write(Utils.unpack(HTTP2.SETTINGS_MAX_FRAME_SIZE, 2));
        settingsStream.write(Utils.unpack(16384, 4));
        return settingsStream.toByteArray();
    }

    private byte[] createResponseHeaders(HPack hp) throws Exception {
        List<String[]> headers = new ArrayList<>();
        headers.add(new String[] { ":status", "200" });
        headers.add(new String[] { "content-type", "application/grpc" });
        byte[] headerData = hp.encode(headers);
        return headerData;
    }

    private byte[] createResponseTrailers(HPack hp) throws Exception {
        List<String[]> headers = new ArrayList<>();
        headers.add(new String[] { "grpc-status", "0" });
        headers.add(new String[] { "grpc-message", "OK" });
        byte[] headerData = hp.encode(headers);
        return headerData;
    }

    private void receiveAcknowledgeFrame(InputStream in) throws Exception {
        HTTP2.Frame frame = new HTTP2.Frame(in);
        if (frame.type != HTTP2.FRAME_TYPE_SETTINGS) {
            throw new Exception("ERROR: Expected SETTINGS frame");
        }
    }

    private void sendAcknowledgeFrame(OutputStream out) throws IOException {
        HTTP2.Frame frame = new HTTP2.Frame(HTTP2.FRAME_TYPE_SETTINGS, 0x0, 0x0);
        frame.serialize(out);
    }
}
