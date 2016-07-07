package com.altai.index;

import com.altai.Utils.HashCodeHelper;

import java.util.HashMap;

/**
 * Created by like on 7/3/16.
 */
public class HugeKeyIndexer {
    // Indexer Setting
    private final String _path;
    private final String _indexNameSuffix;

    private final HashMap<Long, Indexer> _map;

    public HugeKeyIndexer (String path, String indexNameSuffix) {
        _path = path;
        _indexNameSuffix = indexNameSuffix;

        _map = new HashMap<Long, Indexer>();
    }

    public Index get (String key) {
        // compute hash code of (compressed) key
        long keyHashCode = HashCodeHelper.computeToLong(key);

        Indexer indexer = getIndexer(keyHashCode);
        if (indexer == null) {
            return null;
        }

        return indexer.get(key);
    }

    public Index put (String key, Index index) {
        // compute hash code of (compressed) key
        long keyHashCode = HashCodeHelper.computeToLong(key);

        Indexer indexer = getIndexer(keyHashCode);
        if (indexer == null) {
            indexer = new Indexer(_path, keyHashCode, "idx");
            _map.put(keyHashCode, indexer);
        }

        return indexer.put(key, index);
    }

    public Index remove (String key) {
        // compute hash code of (compressed) key
        long keyHashCode = HashCodeHelper.computeToLong(key);

        Indexer indexer = getIndexer(keyHashCode);
        if (indexer == null) {
            // simply return
            return null;
        }

        return indexer.remove(key);
    }

    private Indexer getIndexer(long keyHashCode) {
        return _map.get(keyHashCode);
    }
}
