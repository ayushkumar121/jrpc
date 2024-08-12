package jrpc;

public class Utils {
    public static void printBytes(String prefix, byte[] bytes) {
        System.out.println(prefix);
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        System.out.println(sb);
    }


    public static long pack(byte[] buf) {
        long res = 0;
        for (byte b : buf) {
            res = (res << 8) | (b & 0xff);
        }
        return res;
    }

    public static byte[] unpack(long value, int n) {
        byte[] res = new byte[n];
        for (int i = n - 1; i >= 0; i--) {
            res[i] = (byte) (value & 0xff);
            value >>= 8;
        }
        return res;
    }

}
