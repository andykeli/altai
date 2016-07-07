package com.altai.storage;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Created by like on 7/2/16.
 */
public class Record implements Serializable {
    private ByteBuffer buffer;

    public Record (ByteBuffer buffer) {
        this.buffer = buffer;
        buffer.hashCode();
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public String getValue() {
        if (buffer == null) return null;

        int kSize = buffer.getInt(0);
        int vSize = buffer.getInt(4);

        if (vSize == 0) {
            return null;
        }

        byte[] v = new byte[vSize];
        buffer.get(v, 8 + kSize, vSize);
        return v.toString();
    }

    public Record (String key, String value) {

        // format: kSize(4 bytes), vSize(4 bytes), key(1..n), value(0..n, optional)

        buffer.putInt(key.length());

        int vSize = 0;
        if (value != null) {
            vSize = value.length();
        }

        buffer.putInt(vSize);

        buffer.put(key.getBytes());

        if (value != null) {
            buffer.put(value.getBytes());
        }
    }
}
