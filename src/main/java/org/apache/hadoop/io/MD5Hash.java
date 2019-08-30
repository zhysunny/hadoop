/**
 * Copyright 2005 The Apache Software Foundation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.security.*;

/**
 * 一个可写的MD5哈希值。
 * @author 章云
 * @date 2019/8/30 9:38
 */
public class MD5Hash implements WritableComparable {

    static {
        WritableComparator.define(MD5Hash.class, new Comparator());
    }

    public static final int MD5_LEN = 16;
    private static final MessageDigest DIGESTER;

    static {
        try {
            DIGESTER = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] digest;

    /**
     * 构造一个MD5Hash。
     */
    public MD5Hash() {
        this.digest = new byte[MD5_LEN];
    }

    /**
     * 从十六进制字符串构造md5散列。
     * @param hex
     */
    public MD5Hash(String hex) {
        setDigest(hex);
    }

    /**
     * 构造具有指定值的md5散列。
     * @param digest
     */
    public MD5Hash(byte[] digest) {
        if (digest.length != MD5_LEN) {
            throw new IllegalArgumentException("Wrong length: " + digest.length);
        }
        this.digest = digest;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readFully(digest);
    }

    /**
     * 构造、读取和返回实例。
     */
    public static MD5Hash read(DataInput in) throws IOException {
        MD5Hash result = new MD5Hash();
        result.readFields(in);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(digest);
    }

    /**
     * 将另一个实例的内容复制到这个实例中。
     */
    public void set(MD5Hash that) {
        System.arraycopy(that.digest, 0, this.digest, 0, MD5_LEN);
    }

    /**
     * 返回摘要字节。
     */
    public byte[] getDigest() {
        return digest;
    }

    /**
     * 为字节数组构造哈希值。
     */
    public static MD5Hash digest(byte[] data) {
        return digest(data, 0, data.length);
    }

    /**
     * 为字节数组构造哈希值。
     */
    public static MD5Hash digest(byte[] data, int start, int len) {
        byte[] digest;
        synchronized (DIGESTER) {
            DIGESTER.update(data, start, len);
            digest = DIGESTER.digest();
        }
        return new MD5Hash(digest);
    }

    /**
     * 为字符串构造哈希值。
     */
    public static MD5Hash digest(String string) {
        return digest(UTF8.getBytes(string));
    }

    /**
     * 为字符串构造哈希值。
     */
    public static MD5Hash digest(UTF8 utf8) {
        return digest(utf8.getBytes(), 0, utf8.getLength());
    }

    public long halfDigest() {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((digest[i] & 0xffL) << (8 * (7 - i)));
        }
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MD5Hash)) {
            return false;
        }
        MD5Hash other = (MD5Hash) o;
        return Arrays.equals(this.digest, other.digest);
    }

    @Override
    public int hashCode() {
        return
                (digest[0] | (digest[1] << 8) | (digest[2] << 16) | (digest[3] << 24)) ^
                        (digest[4] | (digest[5] << 8) | (digest[6] << 16) | (digest[7] << 24)) ^
                        (digest[8] | (digest[9] << 8) | (digest[10] << 16) | (digest[11] << 24)) ^
                        (digest[12] | (digest[13] << 8) | (digest[14] << 16) | (digest[15] << 24));
    }


    /**
     * 将此对象与指定的order对象进行比较。
     */
    @Override
    public int compareTo(Object o) {
        MD5Hash that = (MD5Hash) o;
        return WritableComparator.compareBytes(this.digest, 0, MD5_LEN, that.digest, 0, MD5_LEN);
    }

    /**
     * 一个为md5哈希键优化的WritableComparator。
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(MD5Hash.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, MD5_LEN, b2, s2, MD5_LEN);
        }
    }

    private static final char[] HEX_DIGITS =
            {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    @Override
    public String toString() {
        StringBuffer buf = new StringBuffer(MD5_LEN * 2);
        for (int i = 0; i < MD5_LEN; i++) {
            int b = digest[i];
            buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
            buf.append(HEX_DIGITS[b & 0xf]);
        }
        return buf.toString();
    }

    /**
     * 设置十六进制字符串的摘要值。
     */
    public void setDigest(String hex) {
        if (hex.length() != MD5_LEN * 2) {
            throw new IllegalArgumentException("Wrong length: " + hex.length());
        }
        byte[] digest = new byte[MD5_LEN];
        for (int i = 0; i < MD5_LEN; i++) {
            int j = i << 1;
            digest[i] = (byte) (charToNibble(hex.charAt(j)) << 4 | charToNibble(hex.charAt(j + 1)));
        }
        this.digest = digest;
    }

    private static final int charToNibble(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        } else if (c >= 'a' && c <= 'f') {
            return 0xa + (c - 'a');
        } else if (c >= 'A' && c <= 'F') {
            return 0xA + (c - 'A');
        } else {
            throw new RuntimeException("Not a hex character: " + c);
        }
    }


}
