package com.altai.common;

/**
 * Created by like on 7/10/16.
 */
public class Util {
    public static String makeIndexerName (String path, long fileId) {
        return makeFileName(path, fileId, "idx");
    }

    public static String makeFileName (String path, long fileId, String fileNameSuffix) {
        return path + "/" + fileId + "." + fileNameSuffix;
    }
}
