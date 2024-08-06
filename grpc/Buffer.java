package grpc;

public class Buffer {
    private byte[] buffer;
    private int length;
    private int capacity;
    private int position;

    public Buffer() {
        buffer = new byte[1024];
        length = 0;
        position = 0;
        capacity = 1024;
    }

    public Buffer(int capacity) {
        buffer = new byte[capacity];
        length = 0;
        position = 0;
        this.capacity = capacity;
    }

    public Buffer(byte[] buffer) {
        this.buffer = buffer;
        length = buffer.length;
        position = 0;
        capacity = buffer.length;
    }

    public void resize(int newCapacity) {
        byte[] newBuffer = new byte[newCapacity];
        for (int i = 0; i < length; i++) {
            newBuffer[i] = buffer[i];
        }
        buffer = newBuffer;
        capacity = newCapacity;
    }

    public void write(byte b) {
        if (position < length) {
            buffer[position] = b;
        } else {
            if (length == capacity) {
                resize(capacity * 2);
            }
            buffer[length++] = b;
        }
        position++;
    }

    public void write(byte[] bytes) {
        for (byte b: bytes) {
            write(b);
        }
    }

    public byte read() {
        if (length == 0) {
            throw new IndexOutOfBoundsException();
        }

        if (position >= length) {
            throw new IndexOutOfBoundsException();
        }

        return buffer[position++];
    }

    public byte[] read(int n) {
        if (length == 0) {
            throw new IndexOutOfBoundsException();
        }

        if (position + n > length) {
            throw new IndexOutOfBoundsException();
        }

        byte[] result = new byte[n];
        for (int i = 0; i < n; i++) {
            result[i] = buffer[position++];
        }
        return result;
    }

    public void seek(int position) {
        if (position < 0 || position >= length) {
            throw new IndexOutOfBoundsException();
        }
        this.position = position;
    }

    public int available() {
        return length - position;
    }

    public byte[] getBytes() {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = buffer[i];
        }

        return result;
    }
}
