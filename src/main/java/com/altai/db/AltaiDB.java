package com.altai.db;

import com.altai.Utils.HashCodeHelper;
import com.altai.Utils.KeyCompresser;
import com.altai.index.HugeKeyIndexer;
import com.altai.index.Index;
import com.altai.index.Indexer;
import com.altai.storage.Record;
import com.altai.storage.Storage;
import com.sun.prism.impl.Disposer;

/**
 * Created by like on 7/3/16.
 */
public class AltaiDB {
    // CONSTANTS
    private static final int MEGA_BYTE = 0x1000000;

    // DB settings
    private static final int HUGE_KEY_SIZE = 128;
    private static final boolean IS_COMPRESSED = true;

    // indexer stuffs
    private final Indexer _indexer;
    private final HugeKeyIndexer _hkIndexer;

    // storage stuffs
    private final Storage _store;
    private final Storage _hkStore;

    private static final AltaiDB _db = new AltaiDB();

    private AltaiDB () {
        //HUGE_KEY_SIZE = 128;
        //IS_COMPRESSED = true;

        _indexer = new Indexer("/home/like/dev/test_altai/index", 1, "idx");
        _hkIndexer = new HugeKeyIndexer("/home/like/dev/test_altai/hugekey_index", "idx.hk");

        _store = new Storage("/home/like/dev/test_altai", "data", 1 * MEGA_BYTE);
        _hkStore = new Storage("/home/like/dev/test_altai", "data.hk", 5 * MEGA_BYTE);
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
        if (key == null || value == null) {
            return false;
        }

        if (key.length() < HUGE_KEY_SIZE) {
            Record record = new Record (key, value);
            Index idx = _getStorage().putRecord(record);

            Index obsoleteIdx = null;
            if (idx != null) {
                idx.key = key;
                obsoleteIdx = _getIndexer().put(key, idx);
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
                idx.key = savedKey;
                obsoleteIdx = _getHugeKeyIndexer().put(savedKey, idx);
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
