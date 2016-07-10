package com.altai.db;

import com.altai.common.KeyCompresser;
import com.altai.common.Util;
import com.altai.index.HugeKeyIndexer;
import com.altai.index.Index;
import com.altai.index.Indexer;
import com.altai.storage.Record;
import com.altai.storage.Storage;

import java.io.File;


/**
 * Created by like on 7/3/16.
 */
public class AltaiDB {
    // CONSTANTS
    private static final int MEGA_BYTE = 0x1000000;
    private static final String PATH = "/home/like/dev/test_altai";
    private static final String INDEX_PATH = PATH + "/index";
    private static final String HKINDEX_PATH = PATH + "/hugekey_index";
    private static final String DATA_FILE_SUFFIX = "data";

    // DB settings
    private static final int HUGE_KEY_SIZE = 128;
    private static final boolean IS_COMPRESSED = true;

    // indexer stuffs
    private Indexer _indexer;
    private final HugeKeyIndexer _hkIndexer;
    private Indexer _incrementIndexer;
    private HugeKeyIndexer _incrementHkIndexer;

    // storage stuffs
    private final Storage _store;
    private final Storage _hkStore;

    private static final AltaiDB _db = new AltaiDB();

    private AltaiDB () {
        //HUGE_KEY_SIZE = 128;
        //IS_COMPRESSED = true;

        // load storage
        _store = new Storage(PATH, DATA_FILE_SUFFIX, 1 * MEGA_BYTE);
        _hkStore = new Storage(PATH, "data.hk", 5 * MEGA_BYTE);


        // load index from 0.idx
        String indexerFullPathName = Util.makeIndexerName(INDEX_PATH, 0);
        _indexer = Indexer.loadIndexer(indexerFullPathName);
        if (_indexer == null || _indexer.getMap() == null) {
            _indexer = new Indexer (indexerFullPathName);
        }
        _loadIncrementIndexer();
        _dumpIndexer();


        //_indexer = new Indexer("/home/like/dev/test_altai/index", 0, "idx");
        _hkIndexer = new HugeKeyIndexer(HKINDEX_PATH, "idx.hk");

    }

    public static String get(String key) {
        if (key.length() < HUGE_KEY_SIZE) {
            Index idx = _getIndexer().get(key);
            if (idx == null) {
                return null;
            }

            Record record = _getStorage().getRecord(idx);
            if (record == null || record.getBuffer() == null) {
                return null;
            }

            return record.getBuffer().toString();
        }
        else {
            // compress the key if needed
            String savedKey = key;
            if (IS_COMPRESSED) {
                savedKey = KeyCompresser.compress(key);
            }

            Index idx = _getHugeKeyIndexer().get(savedKey);
            if (idx == null) {
                return null;
            }

            Record record = _getHkStorage().getRecord(idx);
            if (record == null || record.getBuffer() == null) {
                return null;
            }

            return record.getBuffer().toString();
        }
    }

    public static boolean put (String key, String value) {
        // key must be not null
        if (key == null) {
            return false;
        }

        // null value means to delete the record

        if (key.length() < HUGE_KEY_SIZE) {
            Record record = new Record (key, value);
            Index idx = _getStorage().putRecord(record);

            Index obsoleteIdx = null;
            if (idx != null) {
                if (value == null) {
                    // to delete from indexer
                    obsoleteIdx = _getIndexer().remove(key);
                }
                else {
                    idx.key = key;
                    obsoleteIdx = _getIndexer().put(key, idx);
                }
            }
            else {
                return false;
            }

            if (obsoleteIdx != null) {
                // save it in toBeDeleted ...
            }

            // do index persistence ...
        }
        else {
            // compress the key if needed
            String savedKey = key;
            if (IS_COMPRESSED) {
                savedKey = KeyCompresser.compress(key);
            }

            Record record = new Record (savedKey, value);
            Index idx = _getHkStorage().putRecord(record);

            Index obsoleteIdx = null;
            if (idx != null) {
                if (value == null) {
                    // to delete from huge key indexer
                    obsoleteIdx = _getHugeKeyIndexer().remove(savedKey);
                }
                else {
                    idx.key = savedKey;
                    obsoleteIdx = _getHugeKeyIndexer().put(savedKey, idx);
                }
            }
            else {
                return false;
            }

            if (obsoleteIdx != null) {
                // save it in toBeDeleted ...
            }

            // do index persistence ...
        }

        return true;
    }

    private void _loadIncrementIndexer() {
        // load all incremental indexers from files
        File file = new File (INDEX_PATH);
        String fileNames[];
        fileNames = file.list();
        if (fileNames.length == 0) {
            return;
        }

        long id;
        for (String fileName : fileNames)
        {
            if(fileName.endsWith(".idx")) {
                id = Long.valueOf(fileName.substring(0, fileName.indexOf(".")));
                Indexer indexer = Indexer.loadIndexer(Util.makeIndexerName(INDEX_PATH, id));
                _indexer.getMap().putAll(indexer.getMap());
            }
        }

        // generate last incremental indexer from data file

    }

    private void _dumpIndexer() {
        // dump Indexer to idx.tmp

        // remove xxx.idx

        // rename idx.tmp to 0.idx
    }

    private static Indexer _getIndexer() {
        return _db._indexer;
    }

    private static HugeKeyIndexer _getHugeKeyIndexer() {
        return _db._hkIndexer;
    }

    private static Storage _getStorage() {
        return _db._store;
    }

    private static Storage _getHkStorage() {
        return _db._hkStore;
    }

}
