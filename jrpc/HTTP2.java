package jrpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HTTP2 {
    public static final byte[] EXPECTED_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes();

    public static final int FRAME_TYPE_DATA = 0x0;
    public static final int FRAME_TYPE_HEADERS = 0x01;
    public static final int FRAME_TYPE_PRIORITY = 0x02;
    public static final int FRAME_TYPE_RST_STREAM = 0x03;
    public static final int FRAME_TYPE_SETTINGS = 0x04;
    public static final int FRAME_TYPE_PING = 0x06;
    public static final int FRAME_TYPE_GOAWAY = 0x07;
    public static final int FRAME_TYPE_WINDOW_UPDATE = 0x08;

    public static final int FLAG_END_HEADERS = 0x04;
    public static final int FLAG_END_STREAM = 0x01;

    public static final int SETTINGS_HEADER_TABLE_SIZE = 0x0;
    public static final int SETTINGS_ENABLE_PUSH = 0x2;
    public static final int SETTINGS_MAX_CONCURRENT_STREAMS = 0x3;
    public static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;
    public static final int SETTINGS_MAX_FRAME_SIZE = 0x5;
    public static final int SETTINGS_MAX_HEADER_LIST_SIZE = 0x6;

    public static final int ERROR_NO_ERROR = 0x0;
    public static final int ERROR_PROTOCOL_ERROR = 0x1;

    public static class Frame {
        public int type;
        public int flag;
        public int streamId;
        public byte[] payload;

        public Frame(int type, int flag, int streamId) {
            this.type = type;
            this.flag = flag;
            this.streamId = streamId;
        }

        public Frame(InputStream in) throws IOException {
            int length = (int) Utils.pack(in.readNBytes(3));
            this.type = (int) Utils.pack(in.readNBytes(1));
            this.flag = (int) Utils.pack(in.readNBytes(1));
            this.streamId = (int) Utils.pack(in.readNBytes(4));
            this.payload = in.readNBytes(length);
        }

        public void serialize(OutputStream out) throws IOException {
            int dataLength = (payload == null) ? 0 : payload.length;
            out.write(Utils.unpack(dataLength, 3));
            out.write((byte) type);
            out.write((byte) flag);
            out.write(Utils.unpack(streamId & 0x7FFFFFFF, 4));
            if (payload != null && payload.length != 0)
                out.write(payload);
            out.flush();
        }
    }
}
