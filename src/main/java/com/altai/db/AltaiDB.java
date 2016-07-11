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
            _indexer = new Indexer(indexerFullPathName);
        }

        // load tmp indexer, and incremental indexer
        boolean loadTmpIndexer = _loadTmpIndexerIfExists();
        boolean loadIncrIndexer = _loadIncrementalIndexer();

        if (loadTmpIndexer || loadIncrIndexer) {
            _dumpIndexer();
        }



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

    private boolean _loadTmpIndexerIfExists() {
        // check if there is idx.tmp
        boolean needDumpIndexer = false;
        String indexerFullPathName = Util.makeIndexerName(INDEX_PATH, 0);
        String tmpIdxName = Util.makeFileName(INDEX_PATH, 0, "idx.tmp");

        File file = new File(tmpIdxName);
        if(file.exists()) {
            Indexer indexer = Indexer.loadIndexer(tmpIdxName);
            if (indexer != null && indexer.getMap() != null) {

                _indexer.getMap().putAll(indexer.getMap());

                // rename 0.idx.tmp to 0.idx
                File idxFile = new File(indexerFullPathName);
                if (!idxFile.exists()) {
                    file.renameTo(new File(indexerFullPathName));
                    // here, we rename indexer file, so do not need to dump it
                }
                else {
                    needDumpIndexer = true;
                }
            }
        }

        return needDumpIndexer;
    }

    private boolean _loadIncrementalIndexer() {
        boolean needDumpIndexer = false;
        // load all incremental indexers from files
        File file = new File (INDEX_PATH);
        String fileNames[];
        fileNames = file.list();
        if (fileNames == null || fileNames.length == 0) {
            return needDumpIndexer;
        }

        long id;
        Indexer indexer;
        for (String fileName : fileNames)
        {
            if(fileName.endsWith(".idx")) {
                id = Long.valueOf(fileName.substring(0, fileName.indexOf(".")));
                if (id == 0) {
                    continue;// skip 0.idx, it is main indexer, not incremental indexer
                }
                indexer = Indexer.loadIndexer(Util.makeIndexerName(INDEX_PATH, id));
                if (indexer != null) {
                    _indexer.getMap().putAll(indexer.getMap());
                    needDumpIndexer = true;
                }
            }
        }

        // generate last incremental indexer from data file
        indexer = _getStorage().makeIndexerForActiveFile();
        if (indexer != null) {
            _indexer.getMap().putAll(indexer.getMap());
            needDumpIndexer = true;
        }

        return needDumpIndexer;
    }

    private void _dumpIndexer() {
        boolean isOk;

        // dump Indexer to idx.tmp
        String tmpFileName = Util.makeFileName(INDEX_PATH, 0, "idx.tmp");
        isOk = Indexer.writeIndexer(tmpFileName, _indexer);
        if (!isOk) {
            System.out.println("dump tmp indexer failure!");
            return;
        }

        // remove xxx.idx
        File file = new File (INDEX_PATH);
        String fileNames[];
        fileNames = file.list();
        if (fileNames != null && fileNames.length != 0) {
            long id;
            Indexer indexer;
            for (String fileName : fileNames)
            {
                if(fileName.endsWith(".idx")) {
                    id = Long.valueOf(fileName.substring(0, fileName.indexOf(".")));
                    String idxName = Util.makeIndexerName(INDEX_PATH, id);
                    File idxFile = new File (idxName);
                    idxFile.delete();
                }
            }
        }

        // rename idx.tmp to 0.idx
        File tmpFile = new File(tmpFileName);
        tmpFile.renameTo(new File(Util.makeIndexerName(INDEX_PATH, 0)));
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
