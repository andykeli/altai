package com.altai.Utils;

import java.nio.ByteBuffer;

/**
 * Created by like on 7/3/16.
 */
public class HashCodeHelper {
    private static final int DEFAULT_CALC_LENGTH = 64;

    public static long computeToLong (String key) {
        return computeToLong(key, DEFAULT_CALC_LENGTH);
    }

    public static long computeToLong(String key, int maxCalcLength) {
        int calcLength = key.length() > maxCalcLength ? maxCalcLength : key.length();

        byte[] bytes = key.substring(0, calcLength-1).getBytes();
        long header = _getHeader(bytes);
        long mid = _getMid(bytes);
        long tail = _getTail(bytes);

        long hashCode = header << 56;
        hashCode += mid << 32;
        hashCode += tail;

        return hashCode;
    }

    private static byte _getHeader(byte[] bytes) {
        // to compute 1 byte header, to perform "exclusive or" on all bytes
        byte header = 0;
        for (byte b: bytes) {
            header ^= b;
        }
        return header;
    }

    private static int _getMid(byte[] bytes) {
        // to compute an integer tail(only the last 3 bytes are considered), to perform "<< and +" on all bytes;
        int mid = 0;
        int numShift = 0;
        for (byte b : bytes) {
            int value = b;
            value = value << numShift;
            mid += value;

            numShift++;
            if (numShift > 16) {
                numShift = 0;
            }
        }

        return mid & 0xFFFFFF;
    }

    private static int _getTail(byte[] bytes) {
        // to compute an integer tail
        int odd = 0, even = 0;
        int numShift = 0;
        boolean isOdd = true;
        for(int i = bytes.length - 1; i >= 0; i--) {
            int value = bytes[i];

            value = value << numShift;
            if (isOdd) {
                odd += value;
                isOdd = false;
            }
            else {
                even += value;
                isOdd = true;
            }

            numShift++;
            if (numShift > 8) {
                numShift = 0;
            }
        }

        return ((odd & 0xFFFF) << 16) | (even & 0xFFFF);
    }
}
