package com.altai.storage;

import com.altai.common.Util;
import com.altai.index.Index;
import com.altai.index.Indexer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by like on 7/2/16.
 */
public class Storage {
    // storage settings
    private final String _path;
    private final String _fileNameSuffix;
    private final int _advisoryMaxSize;
    //private final int READ_BUFFER_SIZE;

    // active file info
    private AtomicInteger _activeFileId;
    private AtomicLong _activeFileSize;
    private String _activeFileName;

    // stuffs for output
    private RandomAccessFile _activeRaf = null;
    private FileChannel _activeFileChannel = null;

    // stuffs for input

/*
    private static final Storage _instance = new Storage();
    private Storage () {};
    public static Storage getInstance() {
        return _instance;
    }
*/

    public Record getRecord(Index idx) {
        RandomAccessFile inRaf = _getInRaf(idx.fileId);
        if (inRaf == null) {
            return null;
        }

        ByteBuffer in = _getInputBuffer(inRaf, idx.offset, idx.size);

        //ByteBuffer buffer = in.duplicate();

        try {
            inRaf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (in == null) {
            return null;
        }

        return new Record(in);
        //return new Record(buffer);
    }

    public Index putRecord (Record r) {
        _createNewActiveFileIfNeeded();

        int offset = -1;
        try {
            offset = (int)_activeFileChannel.size();

            _activeFileChannel.position(offset);

            r.getBuffer().flip();
            _activeFileChannel.write(r.getBuffer());
            _activeFileChannel.force(false);
        }
        catch (IOException e) {
            return null;
        }

        Index idx = null;
        try {
            idx = new Index(null, _activeFileId.get(), (int)_activeFileChannel.size() - offset, offset);
        }
        catch (IOException e) {
            // nothing to do
        }
        return idx;
    }

    public Indexer makeIndexerForActiveFile() {
        RandomAccessFile inRaf = _getInRaf(_activeFileId.get());
        if (inRaf == null) {
            return null;
        }

        int length = 0;
        try {
            length = (int)inRaf.length();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ByteBuffer in = _getInputBuffer(inRaf, 0, length);

        // make indexer
        Indexer indexer = _makeIndexerFromStorage(in, _activeFileId.get());

        try {
            inRaf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return indexer;
    }

    private static Indexer _makeIndexerFromStorage(ByteBuffer buffer, int fileId) {
        if (buffer == null) {
            return null;
        }

        Indexer indexer = new Indexer("dummy");
        boolean hasIndex = false;
        while (buffer.position() != buffer.limit()) {
            Index idx = Record.readRecord(buffer, fileId);
            indexer.put(idx.key, idx);
            hasIndex = true;
        }

        return hasIndex ? indexer : null;
    }

    public Storage (String path, String fileNameSuffix, int advisoryMaxSize) {
        this._path = path;
        this._fileNameSuffix = fileNameSuffix;
        this._advisoryMaxSize = advisoryMaxSize;
        //this.READ_BUFFER_SIZE = readBufferSize;
        _load();
    }

    private void _load () {
        int activeFileId = _getActiveFileId();
        if (activeFileId == -1) {
            _activeFileId.set(1);
            _activeFileSize.set(0);
            return;
        }

        _activeFileId.set(activeFileId);
        File file = new File (_makeActiveFileName());
        _activeFileSize.set(file.length());
    }

    private int _getActiveFileId () {
        File file = new File (_path);
        String fileNames[];
        fileNames = file.list();
        if (fileNames.length == 0) {
            return -1;
        }

        int maxFileId = 0;
        int id;
        for (String fileName : fileNames)
        {
            if(fileName.endsWith(_fileNameSuffix)) {
                //fileName.split(".");
                id = Integer.valueOf(fileName.substring(0, fileName.indexOf(".")));
                if (id > maxFileId) {
                    maxFileId = id;
                }
            }
        }
        return maxFileId;
    }

    private String _makeActiveFileName () {
        _activeFileName = Util.makeFileName(_path, _activeFileId.get(), _fileNameSuffix);
        return _activeFileName;
    }
/*
    private String _makeFileName (int fileId) {
        return _path + "/" + fileId + "." + _fileNameSuffix;
    }
*/
    private void _createNewActiveFileIfNeeded () {
        if (_activeFileSize.get() >= _advisoryMaxSize) {
            _closeActiveFile();

            _activeFileId.getAndIncrement();

            _makeActiveFileName();

            _openActiveFile();
        }
        else if (_activeFileSize.get() == 0) {
            _openActiveFile();
        }
    }

    private void _closeActiveFile () {
        if (_activeFileChannel != null ) {

            try {
                _activeFileChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                _activeFileChannel = null;
            }
        }

        if (_activeRaf != null ) {

            try {
                _activeRaf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                _activeRaf = null;
            }
        }
    }

    private void _openActiveFile() {
        if (_activeRaf != null || _activeFileChannel != null) {
            return;
            //throw new Exception("Active File cannot be opened twice! fileName=" + _activeFileName);
        }

        try {
            _activeRaf = new RandomAccessFile(_activeFileName, "rw");
            _activeFileChannel = _activeRaf.getChannel();
            _activeFileChannel.position(_activeFileChannel.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private RandomAccessFile _getInRaf(int fileId) {
        RandomAccessFile inRaf = null;
        try {
            inRaf = new RandomAccessFile(Util.makeFileName(_path, fileId, _fileNameSuffix), "r");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return inRaf;
    }

    private ByteBuffer _getInputBuffer(RandomAccessFile inRaf, int offset, int size) {
        MappedByteBuffer in = null;
        try {
            in = inRaf.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, size);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return in;
    }
}
