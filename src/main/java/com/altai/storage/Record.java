package com.altai.storage;

import com.altai.index.Index;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Created by like on 7/2/16.
 */
public class Record implements Serializable {
    private ByteBuffer _buffer;

    public Record (ByteBuffer buffer) {
        this._buffer = buffer;
    }

    public ByteBuffer getBuffer() {
        return _buffer;
    }

    public String getValue() {
        if (_buffer == null) return null;

        int kSize = _buffer.getInt(0);
        int vSize = _buffer.getInt(4);

        if (vSize == 0) {
            return null;
        }

        byte[] v = new byte[vSize];
        _buffer.get(v, 8 + kSize, vSize);
        return v.toString();
    }

    public Record (String key, String value) {

        // format: kSize(4 bytes), vSize(4 bytes), key(1..n), value(0..n, optional)

        _buffer.putInt(key.length());

        int vSize = 0;
        if (value != null) {
            vSize = value.length();
        }

        _buffer.putInt(vSize);

        _buffer.put(key.getBytes());

        if (value != null) {
            _buffer.put(value.getBytes());
        }
    }

    public static Index readRecord(ByteBuffer buffer, int fileId) {
        // read a piece of record from buffer(ByteBuffer), then generate its index
        Index idx = null;
        int offset = buffer.position();

        int kSize = buffer.getInt();
        int vSize = buffer.getInt();

        byte[] k = new byte[kSize];
        buffer.get(k, buffer.position(), kSize);

        if (vSize != 0) {
            byte[] v = new byte[vSize];
            buffer.get(v, buffer.position(), vSize);
        }

        return new Index(k.toString(), fileId, buffer.position()-offset, offset);
    }
}
