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
}
