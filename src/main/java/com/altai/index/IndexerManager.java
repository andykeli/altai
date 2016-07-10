package com.altai.index;

/**
 * Created by like on 7/9/16.
 */
public class IndexerManager {
    private Indexer _indexer = null;

    public IndexerManager(Indexer indexer) {
        _indexer = indexer;
    }

    public Indexer getIndexer(String indexerFullPathName) {
        if (_indexer != null) {
            return _indexer;
        }

        // load indexer from file
        _indexer = Indexer.loadIndexer(indexerFullPathName);
        return _indexer;
    }

    public void releaseIndexer() {
        _indexer = null;
    }
}
