package com.altai.index;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by like on 7/3/16.
 */
public class Indexer implements Serializable{
    // Indexer Setting
    private final String _path;
    private final String _indexNameSuffix;

    private long _indexFileId;

    //
    private final HashMap<String, Index> _map;

    public Indexer (String path, long indexFileId, String indexNameSuffix) {
        _path = path;
        _indexFileId = indexFileId;
        _indexNameSuffix = indexNameSuffix;

        _map = new HashMap<String, Index>();
    }

    public Index get (String key) {
        return _map.get(key);
    }

    public Index put (String key, Index index) {
        Index oldIndex = get(key);

        _map.put(key, index);

        return oldIndex;
    }

    public Index remove (String key) {
        Index oldIndex = get(key);

        if (oldIndex == null) {
            return null;
        }

        _map.remove(key);

        return oldIndex;
    }
}
