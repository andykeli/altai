package com.altai.index;

import java.io.Serializable;

/**
 * Created by like on 7/3/16.
 */
public class Index implements Serializable {
    public String key;
    public int fileId;
    public int size;
    public int offset;

    public Index (String key, int fileId, int size, int offset) {
        this.key = key;
        this.fileId = fileId;
        this.size = size;
        this.offset = offset;
    }
}
