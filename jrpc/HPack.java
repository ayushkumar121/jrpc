package jrpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HPack {
    // #region HUFFMAN CODES
    private static String[] huffmanCodes = {
            "1111111111000",
            "11111111111111111011000",
            "1111111111111111111111100010",
            "1111111111111111111111100011",
            "1111111111111111111111100100",
            "1111111111111111111111100101",
            "1111111111111111111111100110",
            "1111111111111111111111100111",
            "1111111111111111111111101000",
            "111111111111111111101010",
            "111111111111111111111111111100",
            "1111111111111111111111101001",
            "1111111111111111111111101010",
            "111111111111111111111111111101",
            "1111111111111111111111101011",
            "1111111111111111111111101100",
            "1111111111111111111111101101",
            "1111111111111111111111101110",
            "1111111111111111111111101111",
            "1111111111111111111111110000",
            "1111111111111111111111110001",
            "1111111111111111111111110010",
            "111111111111111111111111111110",
            "1111111111111111111111110011",
            "1111111111111111111111110100",
            "1111111111111111111111110101",
            "1111111111111111111111110110",
            "1111111111111111111111110111",
            "1111111111111111111111111000",
            "1111111111111111111111111001",
            "1111111111111111111111111010",
            "1111111111111111111111111011",
            "010100",
            "1111111000",
            "1111111001",
            "111111111010",
            "1111111111001",
            "010101",
            "11111000",
            "11111111010",
            "1111111010",
            "1111111011",
            "11111001",
            "11111111011",
            "11111010",
            "010110",
            "010111",
            "011000",
            "00000",
            "00001",
            "00010",
            "011001",
            "011010",
            "011011",
            "011100",
            "011101",
            "011110",
            "011111",
            "1011100",
            "11111011",
            "111111111111100",
            "100000",
            "111111111011",
            "1111111100",
            "1111111111010",
            "100001",
            "1011101",
            "1011110",
            "1011111",
            "1100000",
            "1100001",
            "1100010",
            "1100011",
            "1100100",
            "1100101",
            "1100110",
            "1100111",
            "1101000",
            "1101001",
            "1101010",
            "1101011",
            "1101100",
            "1101101",
            "1101110",
            "1101111",
            "1110000",
            "1110001",
            "1110010",
            "11111100",
            "1110011",
            "11111101",
            "1111111111011",
            "1111111111111110000",
            "1111111111100",
            "11111111111100",
            "100010",
            "111111111111101",
            "00011",
            "100011",
            "00100",
            "100100",
            "00101",
            "100101",
            "100110",
            "100111",
            "00110",
            "1110100",
            "1110101",
            "101000",
            "101001",
            "101010",
            "00111",
            "101011",
            "1110110",
            "101100",
            "01000",
            "01001",
            "101101",
            "1110111",
            "1111000",
            "1111001",
            "1111010",
            "1111011",
            "111111111111110",
            "11111111100",
            "11111111111101",
            "1111111111101",
            "1111111111111111111111111100",
            "11111111111111100110",
            "1111111111111111010010",
            "11111111111111100111",
            "11111111111111101000",
            "1111111111111111010011",
            "1111111111111111010100",
            "1111111111111111010101",
            "11111111111111111011001",
            "1111111111111111010110",
            "11111111111111111011010",
            "11111111111111111011011",
            "11111111111111111011100",
            "11111111111111111011101",
            "11111111111111111011110",
            "111111111111111111101011",
            "11111111111111111011111",
            "111111111111111111101100",
            "111111111111111111101101",
            "1111111111111111010111",
            "11111111111111111100000",
            "111111111111111111101110",
            "11111111111111111100001",
            "11111111111111111100010",
            "11111111111111111100011",
            "11111111111111111100100",
            "111111111111111011100",
            "1111111111111111011000",
            "11111111111111111100101",
            "1111111111111111011001",
            "11111111111111111100110",
            "11111111111111111100111",
            "111111111111111111101111",
            "1111111111111111011010",
            "111111111111111011101",
            "11111111111111101001",
            "1111111111111111011011",
            "1111111111111111011100",
            "11111111111111111101000",
            "11111111111111111101001",
            "111111111111111011110",
            "11111111111111111101010",
            "1111111111111111011101",
            "1111111111111111011110",
            "111111111111111111110000",
            "111111111111111011111",
            "1111111111111111011111",
            "11111111111111111101011",
            "11111111111111111101100",
            "111111111111111100000",
            "111111111111111100001",
            "1111111111111111100000",
            "111111111111111100010",
            "11111111111111111101101",
            "1111111111111111100001",
            "11111111111111111101110",
            "11111111111111111101111",
            "11111111111111101010",
            "1111111111111111100010",
            "1111111111111111100011",
            "1111111111111111100100",
            "11111111111111111110000",
            "1111111111111111100101",
            "1111111111111111100110",
            "11111111111111111110001",
            "11111111111111111111100000",
            "11111111111111111111100001",
            "11111111111111101011",
            "1111111111111110001",
            "1111111111111111100111",
            "11111111111111111110010",
            "1111111111111111101000",
            "1111111111111111111101100",
            "11111111111111111111100010",
            "11111111111111111111100011",
            "11111111111111111111100100",
            "111111111111111111111011110",
            "111111111111111111111011111",
            "11111111111111111111100101",
            "111111111111111111110001",
            "1111111111111111111101101",
            "1111111111111110010",
            "111111111111111100011",
            "11111111111111111111100110",
            "111111111111111111111100000",
            "111111111111111111111100001",
            "11111111111111111111100111",
            "111111111111111111111100010",
            "111111111111111111110010",
            "111111111111111100100",
            "111111111111111100101",
            "11111111111111111111101000",
            "11111111111111111111101001",
            "1111111111111111111111111101",
            "111111111111111111111100011",
            "111111111111111111111100100",
            "111111111111111111111100101",
            "11111111111111101100",
            "111111111111111111110011",
            "11111111111111101101",
            "111111111111111100110",
            "1111111111111111101001",
            "111111111111111100111",
            "111111111111111101000",
            "11111111111111111110011",
            "1111111111111111101010",
            "1111111111111111101011",
            "1111111111111111111101110",
            "1111111111111111111101111",
            "111111111111111111110100",
            "111111111111111111110101",
            "11111111111111111111101010",
            "11111111111111111110100",
            "11111111111111111111101011",
            "111111111111111111111100110",
            "11111111111111111111101100",
            "11111111111111111111101101",
            "111111111111111111111100111",
            "111111111111111111111101000",
            "111111111111111111111101001",
            "111111111111111111111101010",
            "111111111111111111111101011",
            "1111111111111111111111111110",
            "111111111111111111111101100",
            "111111111111111111111101101",
            "111111111111111111111101110",
            "111111111111111111111101111",
            "111111111111111111111110000",
            "11111111111111111111101110",
            "111111111111111111111111111111", // End of line
    };

    private static Map<String, Character> huffmanMap = new HashMap<>();
    static {
        for (int i = 0; i < huffmanCodes.length - 1; i++) {
            huffmanMap.put(huffmanCodes[i], (char) i);
        }
        huffmanMap.put(huffmanCodes[huffmanCodes.length - 1], '\n');
    }
    // #endregion

    // #region STATIC_HEADER_TABLE
    public static final String[][] STATIC_HEADER_TABLE = {
        {":authority", ""},
        {":method", "GET"},
        {":method", "POST"},
        {":path", "/"},
        {":path", "/index.html"},
        {":scheme", "http"},
        {":scheme", "https"},
        {":status", "200"},
        {":status", "204"},
        {":status", "206"},
        {":status", "304"},
        {":status", "400"},
        {":status", "404"},
        {":status", "500"},
        {"accept-charset", ""},
        {"accept-encoding", "gzip, deflate"},
        {"accept-language", ""},
        {"accept-ranges", ""},
        {"accept", ""},
        {"access-control-allow-origin", ""},
        {"age", ""},
        {"allow", ""},
        {"authorization", ""},
        {"cache-control", ""},
        {"content-disposition", ""},
        {"content-encoding", ""},
        {"content-language", ""},
        {"content-length", ""},
        {"content-location", ""},
        {"content-range", ""},
        {"content-type", ""},
        {"cookie", ""},
        {"date", ""},
        {"etag", ""},
        {"expect", ""},
        {"expires", ""},
        {"from", ""},
        {"host", ""},
        {"if-match", ""},
        {"if-modified-since", ""},
        {"if-none-match", ""},
        {"if-range", ""},
        {"if-unmodified-since", ""},
        {"last-modified", ""},
        {"link", ""},
        {"location", ""},
        {"max-forwards", ""},
        {"proxy-authenticate", ""},
        {"proxy-authorization", ""},
        {"range", ""},
        {"referer", ""},
        {"refresh", ""},
        {"retry-after", ""},
        {"server", ""},
        {"set-cookie", ""},
        {"strict-transport-security", ""},
        {"transfer-encoding", ""},
        {"user-agent", ""},
        {"vary", ""},
        {"via", ""},
        {"www-authenticate", ""}
    };
    //#endregion
    private List<String[]> dynamicHeaderTable = new ArrayList<>();

    private static final int PREFIX_MASK_7BITS = 0x7F;
    private static final int PREFIX_MASK_6BITS = 0x3F;
    private static final int PREFIX_MASK_5BITS = 0x1F;
    private static final int PREFIX_MASK_4BITS = 0x0F;

    public HPack() {
        dynamicHeaderTable = new ArrayList<>();
    }

    public List<String[]> decode(byte[] headerBlock) throws Exception {
        List<String[]> decodedHeaderList = new ArrayList<>();
        ByteArrayInputStream in = new ByteArrayInputStream(headerBlock);

        while (in.available() > 0) {
            byte b = (byte) in.read();

            if ((b & 0x80) != 0) {
                // Indexed Header Field Representation
                int headerField = decodeInteger(in, b, PREFIX_MASK_7BITS);
                String headerName = getIndexedHeaderField(headerField)[0];
                String headerValue = getIndexedHeaderField(headerField)[1];

                decodedHeaderList.add(new String[]{headerName, headerValue});
            } else if ((b & 0x40) != 0) {
                // Literal Header Field with Incremental Indexing
                String[] pair =  handleLiteralHeaderField(in, b, PREFIX_MASK_6BITS);
                dynamicHeaderTable.add(pair);
                decodedHeaderList.add(pair);
            } else if ((b & 0xF0) == 0) {
                // Literal Header Field without Indexing
                String[] pair =  handleLiteralHeaderField(in, b, PREFIX_MASK_4BITS);
                decodedHeaderList.add(pair);
            } else if ((b & 0x20) != 0) {
                int newSize = decodeInteger(in, b, PREFIX_MASK_5BITS);
                while (dynamicHeaderTable.size() > newSize) {
                    dynamicHeaderTable.remove(dynamicHeaderTable.size() - 1);
                }
            }
        }

        return decodedHeaderList;
    }

    // TODO: Consider incremental indexing
    public byte[] encode(List<String[]> headers) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (String[] pair: headers) {
            out.write(0); // Literal Header Field without Indexing
            encodeString(out, pair[0]);
            encodeString(out, pair[1]);
        }
        return out.toByteArray();
    }

    private String[] getIndexedHeaderField(int index) {
        if (index == 0) {
            throw new IllegalArgumentException("Index 0 is not used and must be treated as a decoding error.");
        }
    
        if (index <= STATIC_HEADER_TABLE.length) {
            return STATIC_HEADER_TABLE[index - 1]; // Static table is 1-based index
        } else {
            int dynamicIndex = index - STATIC_HEADER_TABLE.length - 1;
            if (dynamicIndex < dynamicHeaderTable.size()) {
                return dynamicHeaderTable.get(dynamicIndex);
            } else {
                throw new IllegalArgumentException("Index out of bounds for dynamic table.");
            }
        }
    }

    private String[] handleLiteralHeaderField(InputStream in, byte b, int prefixMask) throws Exception {
        int headerFieldIndex = decodeInteger(in, b, prefixMask);
        if (headerFieldIndex == 0) {
            String headerName = decodeString(in);
            String headerValue = decodeString(in);
            return new String[] { headerName, headerValue };
        } else {
            String[] field = getIndexedHeaderField(headerFieldIndex);
            String headerValue = decodeString(in);
            return new String[] { field[0], headerValue };
        }
    }

    private String decodeString(InputStream in) throws Exception {
        int b = in.read();
        boolean compressed = (b & 0x80) != 0;

        int length = decodeInteger(in, b, PREFIX_MASK_7BITS);
        byte[] bytes = in.readNBytes(length);

        return compressed ? huffmanDecode(bytes, 0) : new String(bytes);
    }

    private void encodeString(OutputStream out, String str) throws IOException {
        int len = str.length();
        encodeInteger(out, len, PREFIX_MASK_7BITS);
        out.write(str.getBytes());
    }

    private void encodeInteger(OutputStream out, int i, int prefixMask) throws IOException {
        if (i < prefixMask) {
            out.write(i);
        } else {
            int value = i - prefixMask;
            int B;

            do {
                B = value & 127;
                value >>= 7;
                if (value > 0) {
                    B |= 128;
                }
                out.write(B);
            } while (value > 0);
        }
    }

    private int decodeInteger(InputStream in, int b, int prefixMask) throws Exception {
        int prefix = b & prefixMask;

        if (prefix < 127) {
            return prefix;
        }

        int value = 127;
        int M = 0;
        int B;

        do {
            B = in.read();
            value += (B & 127) << M;
            M += 7;
        } while ((B & 128) == 128);

        return value;
    }

    private String huffmanDecode(byte[] bytes, int start) {
        StringBuilder sb = new StringBuilder();
        for (int j = start; j < bytes.length; j++) {
            byte b = bytes[j];
            sb.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
        }
        String binaryString = sb.toString();

        StringBuilder decodedString = new StringBuilder();
        StringBuilder temp = new StringBuilder();
        for (char bit : binaryString.toCharArray()) {
            temp.append(bit);
            if (huffmanMap.containsKey(temp.toString())) {
                decodedString.append(huffmanMap.get(temp.toString()));
                temp.setLength(0);
            }
        }

        return decodedString.toString();
    }
}
