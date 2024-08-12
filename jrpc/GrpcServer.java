package jrpc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jrpc.ProtocolBuffers.Definition;
import jrpc.ProtocolBuffers.ServiceDefinition;
import jrpc.ProtocolBuffers.ServiceMethodDefinition;

public class GrpcServer {
    private static final int PORT = 8080;
    private static final int THREAD_POOL_SIZE = 10;

    private ProtocolBuffers pb;
    private Map<String, GrpcHandler> handlers;
    private Map<String, ServiceMethodDefinition> grpcMethods;

    public GrpcServer(ProtocolBuffers pb, Map<String, GrpcHandler> handlers) throws Exception {
        this.pb = pb;
        this.handlers = handlers;
        this.grpcMethods = new HashMap<>();

        for (Map.Entry<String, Definition> entry : pb.getDefinitions().entrySet()) {
            if (entry.getValue() instanceof ServiceDefinition) {
                ServiceDefinition serviceDefinition = (ServiceDefinition) entry.getValue();
                for (Map.Entry<String, ServiceMethodDefinition> methodEntry : serviceDefinition.methods.entrySet()) {
                    ServiceMethodDefinition method = methodEntry.getValue();
                    grpcMethods.put("/" + entry.getKey() + "/" + methodEntry.getKey(), method);
                }
            }
        }
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

    private void handleClient(Socket client) {
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
            Map<Integer, String> grpcPath = new HashMap<>();
            Map<Integer, MessageObject> requestMap = new HashMap<>();

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

                        for (String[] header : headers) {
                            if (header[0].equals(":path")) {
                                String path = header[1];
                                if (!grpcMethods.containsKey(path)) {
                                    sendGoAwayFrame(out, lastStreamId, HTTP2.ERROR_INTERNAL_ERROR, "Method not found");
                                    terminate = true;
                                    break;
                                }

                                grpcPath.put(frame.streamId, path);
                                break;
                            }
                        }
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

                            String path = grpcPath.get(frame.streamId);
                            ServiceMethodDefinition serviceMethod = grpcMethods.get(path);
                            if (serviceMethod == null) {
                                sendGoAwayFrame(out, lastStreamId, HTTP2.ERROR_INTERNAL_ERROR, "Method not found");
                                terminate = true;
                                break;
                            }

                            MessageObject request = new MessageObject(pb, serviceMethod.inputIdentifier, dataStream);
                            requestMap.put(frame.streamId, request);
                        }

                        if (frame.flag == HTTP2.FLAG_END_STREAM) {
                            MessageObject request = requestMap.get(frame.streamId);
                            if (request == null) {
                                sendGoAwayFrame(out, lastStreamId, HTTP2.ERROR_PROTOCOL_ERROR, "Invalid data frame");
                                terminate = true;
                                break;
                            }

                            try {
                                String path = grpcPath.get(frame.streamId);
                                GrpcHandler handler = handlers.get(path);
                                if (handler == null) {
                                    sendGoAwayFrame(out, lastStreamId, HTTP2.ERROR_INTERNAL_ERROR, "Handler not found");
                                    terminate = true;
                                    break;
                                }

                                MessageObject response = handler.apply(request);
                                ByteArrayOutputStream responseData = new ByteArrayOutputStream();
                                response.serialize(responseData);

                                // Sending response headers
                                HTTP2.Frame responseHeaderFrame = new HTTP2.Frame(
                                    HTTP2.FRAME_TYPE_HEADERS,
                                    HTTP2.FLAG_END_HEADERS, 
                                    frame.streamId);
                                responseHeaderFrame.payload = createResponseHeaders(hp);
                                responseHeaderFrame.serialize(out);

                                // Sending response data
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

                                // Sending response trailers
                                HTTP2.Frame responseTrailerFrame = new HTTP2.Frame(
                                    HTTP2.FRAME_TYPE_HEADERS,
                                    HTTP2.FLAG_END_HEADERS | HTTP2.FLAG_END_STREAM, 
                                    frame.streamId);
                                responseTrailerFrame.payload = createResponseTrailers(hp);
                                responseTrailerFrame.serialize(out);

                                terminate = true;
                            } catch (Exception ex) {
                                sendGoAwayFrame(out, lastStreamId, HTTP2.ERROR_INTERNAL_ERROR, "Internal error");
                                terminate = true;
                                break;
                            }
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
        System.err.println("INFO: Frame received "+ frame.type + " : " + frame.flag + " : " + frame.streamId);
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
