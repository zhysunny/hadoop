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
 * 从内存缓冲区中读取的可重用的{@link DataInput}实现。<br/>
 * 这比每次读取数据时创建新的DataInputStream和ByteArrayInputStream节省了内存。<br/>
 * 典型的用法如下:
 * <pre>
 * DataInputBuffer buffer = new DataInputBuffer();
 * while (... loop condition ...) {
 *     byte[] data = ... get data ...;
 *     int dataLength = ... get data length ...;
 *     buffer.reset(data, dataLength);
 *     ... read buffer using DataInput methods ...
 *  }
 *  </pre>
 * @author 章云
 * @date 2019/8/1 11:40
 */
public class DataInputBuffer extends DataInputStream {

    private static class Buffer extends ByteArrayInputStream {
        public Buffer() {
            super(new byte[]{});
        }

        public void reset(byte[] input, int start, int length) {
            this.buf = input;
            this.count = start + length;
            this.mark = start;
            this.pos = start;
        }

        public int getPosition() {
            return pos;
        }

        public int getLength() {
            return count;
        }
    }

    private Buffer buffer;

    /**
     * 构造一个新的空缓冲区。
     */
    public DataInputBuffer() {
        this(new Buffer());
    }

    private DataInputBuffer(Buffer buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    /**
     * 重置缓冲区读取的数据。
     * @param input
     * @param length
     */
    public void reset(byte[] input, int length) {
        buffer.reset(input, 0, length);
    }

    /**
     * 重置缓冲区读取的数据。
     * @param input
     * @param start
     * @param length
     */
    public void reset(byte[] input, int start, int length) {
        buffer.reset(input, start, length);
    }

    /**
     * 返回输入中的当前位置。
     */
    public int getPosition() {
        return buffer.getPosition();
    }

    /**
     * 返回输入的长度。
     */
    public int getLength() {
        return buffer.getLength();
    }

}
