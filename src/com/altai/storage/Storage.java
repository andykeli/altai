package com.altai.storage;

import com.altai.index.Index;

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
    private RandomAccessFile _inRaf = null;

/*
    private static final Storage _instance = new Storage();
    private Storage () {};
    public static Storage getInstance() {
        return _instance;
    }
*/

    public Record getRecord(Index idx) {
        ByteBuffer in = _getInputBuffer(idx);
        if (in == null) {
            return null;
            //throw new Exception("File cannot be opened! fileId=" + _path + "/" + String.valueOf(rd.fileId) + "." + _fileNameSuffix);
        }

        ByteBuffer buffer = in.duplicate();
        return new Record(buffer);
    }

    public Index putRecord (Record r) throws Exception {
        _createNewActiveFileIfNeeded();

        int offset = (int)_activeFileChannel.size();
        _activeFileChannel.position(_activeFileChannel.size());

        r.getBuffer().flip();
        _activeFileChannel.write(r.getBuffer());
        _activeFileChannel.force(false);

        Index idx = new Index(null, _activeFileId.get(), (int)_activeFileChannel.size() - offset, offset);
        return idx;
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
                fileName.split(".");
                id = Integer.valueOf(fileName.substring(0, fileName.indexOf(".")));
                if (id > maxFileId) {
                    maxFileId = id;
                }
            }
        }
        return maxFileId;
    }

    private String _makeActiveFileName () {
        _activeFileName = _makeFileName(_activeFileId.get());
        return _activeFileName;
    }

    private String _makeFileName (int fileId) {
        return _path + "/" + fileId + "." + _fileNameSuffix;
    }

    private void _createNewActiveFileIfNeeded () throws Exception {
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

    private void _openActiveFile() throws Exception {
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

    private ByteBuffer _getInputBuffer(Index idx) {
        MappedByteBuffer in = null;
        try {
            if (_inRaf != null) {
                _inRaf.close();
                _inRaf = null;
            }

            _inRaf = new RandomAccessFile(_makeFileName(idx.fileId), "r");
            in = _inRaf.getChannel().map(FileChannel.MapMode.READ_ONLY, idx.offset, idx.size);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return in;
    }
}
