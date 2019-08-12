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

/**
 * 一个可重用的{@link DataOutput}实现，它写入内存缓冲区。<br/>
 * 这节省了每次写入数据时创建新的DataOutputStream和ByteArrayOutputStream的内存。<br/>
 * 典型的用法如下:
 * <pre>
 * DataOutputBuffer buffer = new DataOutputBuffer();
 * while (... loop condition ...) {
 *     buffer.reset();
 *     ... write buffer using DataOutput methods ...
 *     byte[] data = buffer.getData();
 *     int dataLength = buffer.getLength();
 *     ... write data to its ultimate destination ...
 * }
 * </pre>
 * @author 章云
 * @date 2019/8/1 11:08
 */
public class DataOutputBuffer extends DataOutputStream {

    private static class Buffer extends ByteArrayOutputStream {
        public byte[] getData() {
            return buf;
        }

        public int getLength() {
            return count;
        }

        @Override
        public void reset() {
            count = 0;
        }

        public void write(DataInput in, int len) throws IOException {
            int newCount = count + len;
            if (newCount > buf.length) {
                byte[] newbuf = new byte[Math.max(buf.length << 1, newCount)];
                System.arraycopy(buf, 0, newbuf, 0, count);
                buf = newbuf;
            }
            in.readFully(buf, count, len);
            count = newCount;
        }
    }

    private Buffer buffer;

    /**
     * 构造一个新的空缓冲区。
     */
    public DataOutputBuffer() {
        this(new Buffer());
    }

    private DataOutputBuffer(Buffer buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    /**
     * 返回缓冲区的当前内容。<br/>
     * 数据只对{@link #getLength()}有效。
     * @return
     */
    public byte[] getData() {
        return buffer.getData();
    }

    /**
     * 返回缓冲区中当前有效数据的长度。
     */
    public int getLength() {
        return buffer.getLength();
    }

    /**
     * 将缓冲区重置为空。
     */
    public DataOutputBuffer reset() {
        this.written = 0;
        buffer.reset();
        return this;
    }

    /**
     * 将DataInput中的字节直接写入缓冲区。
     * @param in
     * @param length
     * @throws IOException
     */
    public void write(DataInput in, int length) throws IOException {
        buffer.write(in, length);
    }
}
