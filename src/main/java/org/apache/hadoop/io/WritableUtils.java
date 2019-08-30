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

import java.io.*;

import org.apache.hadoop.mapred.JobConf;

import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class WritableUtils {

    public static byte[] readCompressedByteArray(DataInput in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        byte[] buffer = new byte[length];
        // 可以/应该使用readFully(缓冲区,0,长度)?
        in.readFully(buffer);
        GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buffer, 0, buffer.length));
        byte[] outbuf = new byte[length];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int len;
        while ((len = gzi.read(outbuf, 0, outbuf.length)) != -1) {
            bos.write(outbuf, 0, len);
        }
        byte[] decompressed = bos.toByteArray();
        bos.close();
        gzi.close();
        return decompressed;
    }

    public static void skipCompressedByteArray(DataInput in) throws IOException {
        int length = in.readInt();
        if (length != -1) {
            in.skipBytes(length);
        }
    }

    public static int writeCompressedByteArray(DataOutput out, byte[] bytes) throws IOException {
        if (bytes != null) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gzout = new GZIPOutputStream(bos);
            gzout.write(bytes, 0, bytes.length);
            gzout.close();
            byte[] buffer = bos.toByteArray();
            int len = buffer.length;
            out.writeInt(len);
            out.write(buffer, 0, len);
            // 调试!我们一旦有了信心，就会失去这个。
            return ((bytes.length != 0) ? (100 * buffer.length) / bytes.length : 0);
        } else {
            out.writeInt(-1);
            return -1;
        }
    }


    public static String readCompressedString(DataInput in) throws IOException {
        byte[] bytes = readCompressedByteArray(in);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, "UTF-8");
    }


    public static int writeCompressedString(DataOutput out, String s) throws IOException {
        return writeCompressedByteArray(out, (s != null) ? s.getBytes("UTF-8") : null);
    }

    /**
     * 将字符串写成网络Int n，后跟n个字节<br/>
     * 替代16位读/写UTF。<br/>
     * 编码标准是……吗?
     * @param out
     * @param s
     * @throws IOException
     */
    public static void writeString(DataOutput out, String s) throws IOException {
        if (s != null) {
            byte[] buffer = s.getBytes("UTF-8");
            int len = buffer.length;
            out.writeInt(len);
            out.write(buffer, 0, len);
        } else {
            out.writeInt(-1);
        }
    }

    /**
     * 读取一个字符串作为网络Int n，后面跟着n个字节<br/>
     * 替代16位读/写UTF。<br/>
     * 编码标准是……吗?
     * @param in
     * @return
     * @throws IOException
     */
    public static String readString(DataInput in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        byte[] buffer = new byte[length];
        in.readFully(buffer);
        return new String(buffer, "UTF-8");
    }


    /**
     * 将字符串数组写成Nework Int N，后面跟着Int N字节数组字符串。<br/>
     * 可以使用内省来概括。
     * @param out
     * @param s
     * @throws IOException
     */
    public static void writeStringArray(DataOutput out, String[] s) throws IOException {
        out.writeInt(s.length);
        for (int i = 0; i < s.length; i++) {
            writeString(out, s[i]);
        }
    }

    /**
     * 将一个字符串数组写成Nework Int N，后面跟着一个压缩字符串的Int N字节数组。<br/>
     * 还处理空数组和空值。<br/>
     * 可以使用内省来概括。
     * @param out
     * @param s
     * @throws IOException
     */
    public static void writeCompressedStringArray(DataOutput out, String[] s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(s.length);
        for (int i = 0; i < s.length; i++) {
            writeCompressedString(out, s[i]);
        }
    }

    /**
     * 将字符串数组写成Nework Int N，后面跟着Int N字节数组字符串。<br/>
     * 可以使用内省来概括。实际上这一点不能…
     * @param in
     * @return
     * @throws IOException
     */
    public static String[] readStringArray(DataInput in) throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        String[] s = new String[len];
        for (int i = 0; i < len; i++) {
            s[i] = readString(in);
        }
        return s;
    }


    /**
     * 将字符串数组写成Nework Int N，后面跟着Int N字节数组字符串。<br/>
     * 可以使用内省来概括。<br/>
     * 处理空数组和空值。
     * @param in
     * @return
     * @throws IOException
     */
    public static String[] readCompressedStringArray(DataInput in) throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        String[] s = new String[len];
        for (int i = 0; i < len; i++) {
            s[i] = readCompressedString(in);
        }
        return s;
    }


    /**
     * 测试实用程序方法显示字节数组。
     * @param record
     */
    public static void displayByteArray(byte[] record) {
        int i;
        for (i = 0; i < record.length - 1; i++) {
            if (i % 16 == 0) {
                System.out.println();
            }
            System.out.print(Integer.toHexString(record[i] >> 4 & 0x0F));
            System.out.print(Integer.toHexString(record[i] & 0x0F));
            System.out.print(",");
        }
        System.out.print(Integer.toHexString(record[i] >> 4 & 0x0F));
        System.out.print(Integer.toHexString(record[i] & 0x0F));
        System.out.println();
    }

    /**
     * 用于克隆可写项的一对输入/输出缓冲区。
     */
    private static class CopyInCopyOutBuffer {
        DataOutputBuffer outBuffer = new DataOutputBuffer();
        DataInputBuffer inBuffer = new DataInputBuffer();

        /**
         * 将数据从输出缓冲区移动到输入缓冲区。
         */
        void moveData() {
            inBuffer.reset(outBuffer.getData(), outBuffer.getLength());
        }
    }

    /**
     * 为每个试图克隆对象的线程分配一个缓冲区。
     */
    private static ThreadLocal cloneBuffers = new ThreadLocal() {
        @Override
        protected synchronized Object initialValue() {
            return new CopyInCopyOutBuffer();
        }
    };

    /**
     * 使用序列化将可写对象复制到缓冲区。
     * @param orig 要复制的对象
     * @return 复制的对象
     */
    public static Writable clone(Writable orig, JobConf conf) {
        try {
            Writable newInst = (Writable) conf.newInstance(orig.getClass());
            CopyInCopyOutBuffer buffer = (CopyInCopyOutBuffer) cloneBuffers.get();
            buffer.outBuffer.reset();
            orig.write(buffer.outBuffer);
            buffer.moveData();
            newInst.readFields(buffer.inBuffer);
            return newInst;
        } catch (IOException e) {
            throw new RuntimeException("Error writing/reading clone buffer", e);
        }
    }

}
