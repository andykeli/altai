package com.altai.index;

import com.altai.common.HashCodeHelper;
import com.altai.common.Util;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by like on 7/3/16.
 */
public class HugeKeyIndexer implements Serializable {
    // Indexer Setting
    private final String _path;
    private final String _indexNameSuffix;

    private final HashMap<Long, IndexerManager> _map;

    public HugeKeyIndexer (String path, String indexNameSuffix) {
        _path = path;
        _indexNameSuffix = indexNameSuffix;

        _map = new HashMap<Long, IndexerManager>();
    }

    public Index get (String key) {
        // compute hash code of (compressed) key
        long keyHashCode = HashCodeHelper.computeToLong(key);

        IndexerManager im = _getIndexerManager(keyHashCode);
        if (im == null) {
            return null;
        }

        Indexer indexer = im.getIndexer(Util.makeIndexerName(_path, keyHashCode));
        if (indexer == null) {
            return null;
        }

        im.releaseIndexer();
        return indexer.get(key);
    }

    public Index put (String key, Index index) {
        // compute hash code of (compressed) key
        long keyHashCode = HashCodeHelper.computeToLong(key);
        String indexerFullPathName = Util.makeIndexerName(_path, keyHashCode);

        IndexerManager im = _getIndexerManager(keyHashCode);
        if (im == null) {
            im = new IndexerManager(new Indexer(indexerFullPathName));
            _map.put(keyHashCode, im);
        }

        Indexer indexer = im.getIndexer(indexerFullPathName);
        if (indexer == null) {
            return null;
        }

        im.releaseIndexer();

        Index obsoleteIdx = indexer.put(key, index);

        Indexer.writeIndexer(indexerFullPathName, indexer);
        return obsoleteIdx;
    }

    public Index remove (String key) {
        // compute hash code of (compressed) key
        long keyHashCode = HashCodeHelper.computeToLong(key);
        String indexerFullPathName = Util.makeIndexerName(_path,keyHashCode);

        IndexerManager im = _getIndexerManager(keyHashCode);
        if (im == null) {
            // simply return
            return null;
        }

        Indexer indexer = im.getIndexer(indexerFullPathName);
        if (indexer == null) {
            return null;
        }

        im.releaseIndexer();

        Index obsoleteIdx = indexer.remove(key);

        if (indexer.isEmpty()) {
            im = _map.remove(keyHashCode);
            // to do, write im to needToDelete
            File file = new File(indexerFullPathName);
            file.delete();
        }
        else {
            Indexer.writeIndexer(indexerFullPathName, indexer);
        }

        return obsoleteIdx;
    }

    private IndexerManager _getIndexerManager(long keyHashCode) {
        return _map.get(keyHashCode);
    }
}
