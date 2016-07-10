package com.altai.index;

import com.altai.common.Serialization;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Created by like on 7/3/16.
 */
public class Indexer implements Serializable{
    // Indexer Setting
    private final String _fullPathName;
    /*
    private final String _path;
    private final String _indexNameSuffix;
    private long _indexFileId;*/

    //
    private final HashMap<String, Index> _map;

    public Indexer (String indexerFullPathName) {
        _fullPathName = indexerFullPathName;
        /*
        _path = path;
        _indexFileId = indexFileId;
        _indexNameSuffix = indexNameSuffix; */

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

    public boolean isEmpty() {
        return _map.isEmpty();
    }

    public HashMap<String, Index> getMap() { return _map; }

    public static Indexer loadIndexer(String indexerFullPathName)
    {
        // load indexer from file
        Object obj = Serialization.readFromFile(indexerFullPathName);
        if (obj instanceof Indexer) {
            return (Indexer) obj;
        }
        else {
            return null;
        }
    }

    public static boolean writeIndexer(String indexerFullPathName, Indexer indexer) {
        //Write Obj to File
        return Serialization.writeToFile(indexerFullPathName, indexer);
    }

    public static Indexer makeIndexerFromStorage(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }


        return null;
    }
}
