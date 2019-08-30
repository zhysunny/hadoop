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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * 使用UTF8编码的字符串的WritableComparable。<br/>
 * 还包括有效读写UTF-8的实用程序。<br/>
 * 实际存储的是字符串的字节数组和长度
 * @author 章云
 * @date 2019/8/1 9:19
 */
public class UTF8 implements WritableComparable<UTF8> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UTF8.class);

    private static final DataOutputBuffer OBUF = new DataOutputBuffer();
    private static final DataInputBuffer IBUF = new DataInputBuffer();

    private static final byte[] EMPTY_BYTES = new byte[0];

    private byte[] bytes = EMPTY_BYTES;
    private int length;

    static {
        // 注册这个比较器
        WritableComparator.define(UTF8.class, new Comparator());
    }

    public UTF8() {
    }

    /**
     * 从给定字符串构造。
     * @param string
     */
    public UTF8(String string) {
        set(string);
    }

    /**
     * 从给定字符串构造。
     * @param utf8
     */
    public UTF8(UTF8 utf8) {
        set(utf8);
    }

    /**
     * 原始字节。
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * 编码字符串中的字节数。
     */
    public int getLength() {
        return length;
    }

    /**
     * 设置为包含字符串的内容。
     * @param string
     */
    public void set(String string) {
        if (string.length() > 0xffff / 3) {
            // 0xffff = 65535
            // 0xffff / 3 = 21845
            // maybe too long
            LOGGER.warn("truncating long string: " + string.length() + " chars, starting with " + string.substring(0, 20));
            string = string.substring(0, 0xffff / 3);
        }
        // 计算这段字符串所需的字节数。
        length = utf8Length(string);
        if (length > 0xffff) {
            // 二次检查长度
            throw new RuntimeException("string too long!");
        }
        if (bytes == null || length > bytes.length) {
            // 增加缓冲
            bytes = new byte[length];
        }
        try {
            synchronized (OBUF) {
                // 避免同步会分配
                OBUF.reset();
                writeChars(OBUF, string, 0, string.length());
                System.arraycopy(OBUF.getData(), 0, bytes, 0, length);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 设置为包含字符串的内容。
     * @param other
     */
    public void set(UTF8 other) {
        length = other.length;
        if (bytes == null || length > bytes.length) {
            // 增加缓冲
            bytes = new byte[length];
        }
        System.arraycopy(other.bytes, 0, bytes, 0, length);
    }

    /**
     * 读取数据输入流，覆盖原utf8存储的字符串
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        // 读取输入流字节长度
        length = in.readUnsignedShort();
        if (bytes == null || bytes.length < length) {
            bytes = new byte[length];
        }
        // 读取输入流字节
        in.readFully(bytes, 0, length);
    }

    /**
     * 跳过输入中的一个UTF8。<br/>
     * readUnsignedShort相当于返回数据流长度，跳过这个长度数据流就为空，相当于清空数据流
     * @param in
     * @throws IOException
     */
    public static void skip(DataInput in) throws IOException {
        int length = in.readUnsignedShort();
        in.skipBytes(length);
    }

    /**
     * 写的过程中不改变本身UTF8存储的字符串<br/>
     * 相当于把UTF8存储的字符串追加到out输出流中，是追加，不是覆盖
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(length);
        out.write(bytes, 0, length);
    }

    /**
     * 按照字节数组元素比较
     */
    @Override
    public int compareTo(UTF8 that) {
        return WritableComparator.compareBytes(bytes, 0, length,
                that.bytes, 0, that.length);
    }

    @Override
    public String toString() {
        // 等价于new String(bytes,"UTF-8")
        StringBuffer buffer = new StringBuffer(length);
        try {
            synchronized (IBUF) {
                IBUF.reset(bytes, length);
                readChars(IBUF, buffer, length);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buffer.toString();
    }

    /**
     * 字节数组元素相等或字符串相等返回true<br/>
     * 该对象作为Map的key必须重写equals和hashCode方法
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof UTF8)) {
            return false;
        }
        UTF8 that = (UTF8) o;
        if (this.length != that.length) {
            return false;
        } else {
            return WritableComparator.compareBytes(bytes, 0, length,
                    that.bytes, 0, that.length) == 0;
        }
    }

    /**
     * 该对象作为Map的key必须重写equals和hashCode方法
     * @return
     */
    @Override
    public int hashCode() {
        return WritableComparator.hashBytes(bytes, length);
    }

    /**
     * 一个为UTF8键优化的WritableComparator。
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(UTF8.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            // l1,l2没用到
            int n1 = readUnsignedShort(b1, s1);
            int n2 = readUnsignedShort(b2, s2);
            return compareBytes(b1, s1 + 2, n1, b2, s2 + 2, n2);
        }
    }

    /// 下面是静态实用程序

    /// 这些可能不再经常使用，并可能被删除…

    /**
     * 将字符串转换为UTF-8编码的字节数组。
     * @param string
     * @return
     */
    public static byte[] getBytes(String string) {
        // 相当于string.getBytes("UTF-8");
        byte[] result = new byte[utf8Length(string)];
        try {
            synchronized (OBUF) {
                OBUF.reset();
                writeChars(OBUF, string, 0, string.length());
                System.arraycopy(OBUF.getData(), 0, result, 0, OBUF.getLength());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * 读取UTF-8编码的字符串。
     * @param in
     * @return
     * @throws IOException
     */
    public static String readString(DataInput in) throws IOException {
        int bytes = in.readUnsignedShort();
        StringBuffer buffer = new StringBuffer(bytes);
        readChars(in, buffer, bytes);
        return buffer.toString();
    }

    /**
     * 读取数据输入流(in)一定长度(nBytes)的字节数组,存入buffer
     * @param in
     * @param buffer
     * @param nBytes
     * @throws IOException
     */
    private static void readChars(DataInput in, StringBuffer buffer, int nBytes) throws IOException {
        synchronized (OBUF) {
            OBUF.reset();
            OBUF.write(in, nBytes);
            byte[] bytes = OBUF.getData();
            int i = 0;
            while (i < nBytes) {
                byte b = bytes[i++];
                if ((b & 0x80) == 0) {
                    buffer.append((char) (b & 0x7F));
                } else if ((b & 0xE0) != 0xE0) {
                    buffer.append((char) (((b & 0x1F) << 6)
                            | (bytes[i++] & 0x3F)));
                } else {
                    buffer.append((char) (((b & 0x0F) << 12)
                            | ((bytes[i++] & 0x3F) << 6)
                            | (bytes[i++] & 0x3F)));
                }
            }
        }
    }

    /**
     * 编写一个UTF-8编码的字符串。
     * @param out
     * @param string
     * @return
     * @throws IOException
     */
    public static int writeString(DataOutput out, String string) throws IOException {
        if (string.length() > 0xffff / 3) {
            // 0xffff = 65535
            // 0xffff / 3 = 21845
            // maybe too long
            LOGGER.warn("truncating long string: " + string.length() + " chars, starting with " + string.substring(0, 20));
            string = string.substring(0, 0xffff / 3);
        }
        // 计算这段字符串所需的字节数。
        int len = utf8Length(string);
        if (len > 0xffff) {
            // 二次检查长度
            throw new IOException("string too long!");
        }
        out.writeShort(len);
        writeChars(out, string, 0, string.length());
        return len;
    }

    /**
     * 返回写这段字符串所需的字节数。
     * @param string
     * @return
     */
    private static int utf8Length(String string) {
        int stringLength = string.length();
        int utf8Length = 0;
        for (int i = 0; i < stringLength; i++) {
            int c = string.charAt(i);
            if ((c >= 1) && (c <= 127)) {
                // 字节(一般是字母)
                utf8Length++;
            } else if (c > 2047) {
                // 一般是中文汉字
                utf8Length += 3;
            } else {
                utf8Length += 2;
            }
        }
        return utf8Length;
    }

    /**
     * 将字符串写入到输出流
     * @param out
     * @param string
     * @param start
     * @param length
     * @throws IOException
     */
    private static void writeChars(DataOutput out, String string, int start, int length) throws IOException {
        final int end = start + length;
        for (int i = start; i < end; i++) {
            int code = string.charAt(i);
            if (code >= 0x01 && code <= 0x7F) {
                out.writeByte((byte) code);
            } else if (code <= 0x07FF) {
                out.writeByte((byte) (0xC0 | ((code >> 6) & 0x1F)));
                out.writeByte((byte) (0x80 | code & 0x3F));
            } else {
                out.writeByte((byte) (0xE0 | ((code >> 12) & 0X0F)));
                out.writeByte((byte) (0x80 | ((code >> 6) & 0x3F)));
                out.writeByte((byte) (0x80 | (code & 0x3F)));
            }
        }
    }

}
