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
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Constants;

import java.io.*;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * 该实用程序将{@link FSOutputStream}包装在{@link DataOutputStream}中，通过{@link BufferedOutputStream}缓冲输出，并创建一个校验文件。
 * @author 章云
 * @date 2019/8/2 16:33
 */
public class FSDataOutputStream extends DataOutputStream {
    public static final byte[] CHECKSUM_VERSION = new byte[]{'c', 'r', 'c', 0};

    /**
     * 存储数据的校验和。
     */
    private static class Summer extends FilterOutputStream {

        private FSDataOutputStream sums;
        private Checksum sum = new CRC32();
        private int inSum;
        private int bytesPerSum;

        public Summer(FileSystem fs, File file, boolean overwrite, Configuration conf) throws IOException {
            super(fs.createRaw(file, overwrite));
            this.bytesPerSum = Constants.IO_BYTES_PER_CHECKSUM;
            this.sums = new FSDataOutputStream(fs.createRaw(fs.getChecksumFile(file), true), conf);
            sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
            sums.writeInt(this.bytesPerSum);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            int summed = 0;
            while (summed < len) {

                int goal = this.bytesPerSum - inSum;
                int inBuf = len - summed;
                int toSum = inBuf <= goal ? inBuf : goal;

                sum.update(b, off + summed, toSum);
                summed += toSum;

                inSum += toSum;
                if (inSum == this.bytesPerSum) {
                    writeSum();
                }
            }

            out.write(b, off, len);
        }

        private void writeSum() throws IOException {
            if (inSum != 0) {
                sums.writeInt((int) sum.getValue());
                sum.reset();
                inSum = 0;
            }
        }

        @Override
        public void close() throws IOException {
            writeSum();
            sums.close();
            super.close();
        }

    }

    private static class PositionCache extends FilterOutputStream {
        long position;

        public PositionCache(OutputStream out) throws IOException {
            super(out);
        }

        // 这是BufferedOutputStream惟一调用的write()方法，因此我们捕捉对它的调用，以便缓存位置。
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            // 更新位置
            position += len;
        }

        public long getPos() {
            // 返回缓存的位置
            return position;
        }

    }

    private static class Buffer extends BufferedOutputStream {
        public Buffer(OutputStream out, int bufferSize) {
            super(out, bufferSize);
        }

        public long getPos() {
            return ((PositionCache) out).getPos() + this.count;
        }

        /**
         * 优化版本的write(int)
         * @param b
         * @throws IOException
         */
        @Override
        public void write(int b) throws IOException {
            if (count >= buf.length) {
                super.write(b);
            } else {
                buf[count++] = (byte) b;
            }
        }

    }

    public FSDataOutputStream(FileSystem fs, File file, boolean overwrite, Configuration conf, int bufferSize) throws IOException {
        super(new Buffer(new PositionCache(new Summer(fs, file, overwrite, conf)), bufferSize));
    }

    /**
     * 没有校验的构造器。
     */
    private FSDataOutputStream(FSOutputStream out, Configuration conf) throws IOException {
        this(out, Constants.IO_FILE_BUFFER_SIZE);
    }

    /**
     * 没有校验的构造器。
     */
    private FSDataOutputStream(FSOutputStream out, int bufferSize) throws IOException {
        super(new Buffer(new PositionCache(out), bufferSize));
    }

    public long getPos() {
        return ((Buffer) out).getPos();
    }

}
