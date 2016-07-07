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

        byte[] v = new byte[vSize];
        buffer.get(v, 8 + kSize, vSize);
        return v.toString();
    }

    public Record (String key, String value) {
        buffer.putInt(key.length());
        buffer.putInt(value.length());
        buffer.put(key.getBytes());
        buffer.put(value.getBytes());
    }
}
