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

import java.io.*;
import java.util.Arrays;
import java.util.zip.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.exception.ChecksumException;
import org.apache.hadoop.util.ConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 该实用程序将{@link FSInputStream}包装在{@link DataInputStream}中，并通过{@link BufferedInputStream}缓冲输入。
 * @author 章云
 * @date 2019/8/2 15:47
 */
public class FSDataInputStream extends DataInputStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(FSDataInputStream.class);

    private static final byte[] VERSION = FSDataOutputStream.CHECKSUM_VERSION;
    private static final int HEADER_LENGTH = 8;

    private int bytesPerSum = 1;

    /**
     * 验证数据是否匹配校验。
     */
    private class Checker extends FilterInputStream implements Seekable {
        private FileSystem fs;
        private File file;
        private FSDataInputStream sums;
        private Checksum sum = new CRC32();
        private int inSum;

        public Checker(FileSystem fs, File file, Configuration conf) throws IOException {
            super(fs.openRaw(file));
            this.fs = fs;
            this.file = file;
            File sumFile = fs.getChecksumFile(file);
            try {
                this.sums = new FSDataInputStream(fs.openRaw(sumFile), conf);
                byte[] version = new byte[VERSION.length];
                sums.readFully(version);
                if (!Arrays.equals(version, VERSION)) {
                    throw new IOException("Not a checksum file: " + sumFile);
                }
                bytesPerSum = sums.readInt();
            } catch (FileNotFoundException e) {
                stopSumming();
            } catch (IOException e) {
                LOGGER.warn("Problem opening checksum file: " + file + ".  Ignoring with exception " + e + ".");
                stopSumming();
            }
        }

        @Override
        public void seek(long desired) throws IOException {
            ((Seekable) in).seek(desired);
            if (sums != null) {
                if (desired % bytesPerSum != 0) {
                    throw new IOException("Seek to non-checksummed position.");
                }
                try {
                    sums.seek(HEADER_LENGTH + 4 * (desired / bytesPerSum));
                } catch (IOException e) {
                    LOGGER.warn("Problem seeking checksum file: " + e + ". Ignoring.");
                    stopSumming();
                }
                sum.reset();
                inSum = 0;
            }
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            int read = in.read(b, off, len);
            if (sums != null) {
                int summed = 0;
                while (summed < read) {
                    int goal = bytesPerSum - inSum;
                    int inBuf = read - summed;
                    int toSum = inBuf <= goal ? inBuf : goal;
                    sum.update(b, off + summed, toSum);
                    summed += toSum;
                    inSum += toSum;
                    if (inSum == bytesPerSum) {
                        verifySum(read - (summed - bytesPerSum));
                    }
                }
            }
            return read;
        }

        private void verifySum(int delta) throws IOException {
            int crc;
            try {
                crc = sums.readInt();
            } catch (IOException e) {
                LOGGER.warn("Problem reading checksum file: " + e + ". Ignoring.");
                stopSumming();
                return;
            }
            int sumValue = (int) sum.getValue();
            sum.reset();
            inSum = 0;
            if (crc != sumValue) {
                long pos = getPos() - delta;
                fs.reportChecksumFailure(file, (FSInputStream) in, pos, bytesPerSum, crc);
                throw new ChecksumException("Checksum error: " + file + " at " + pos);
            }
        }

        public long getPos() throws IOException {
            return ((FSInputStream) in).getPos();
        }

        @Override
        public void close() throws IOException {
            super.close();
            stopSumming();
        }

        private void stopSumming() {
            if (sums != null) {
                try {
                    sums.close();
                } catch (IOException f) {
                }
                sums = null;
                bytesPerSum = 1;
            }
        }
    }

    /**
     * 缓存文件位置。这显著提高了性能。
     */
    private static class PositionCache extends FilterInputStream {
        long position;

        public PositionCache(InputStream in) {
            super(in);
        }

        // 这是BufferedInputStream调用的惟一read()方法，因此我们捕捉对它的调用，以便缓存位置。
        @Override
        public int read(byte b[], int off, int len) throws IOException {
            int result;
            if ((result = in.read(b, off, len)) > 0) {
                position += result;
            }
            return result;
        }

        public void seek(long desired) throws IOException {
//            寻找潜在的流
            ((Seekable) in).seek(desired);
//            更新位置
            position = desired;
        }

        public long getPos() throws IOException {
//            返回缓存的位置
            return position;
        }

    }

    /**
     * 输入缓冲区。这显著提高了性能。
     */
    private class Buffer extends BufferedInputStream {
        public Buffer(PositionCache in, int bufferSize) {
            super(in, bufferSize);
        }

        public void seek(long desired) throws IOException {
            long end = ((PositionCache) in).getPos();
            long start = end - this.count;
            if (desired >= start && desired < end) {
                // 可以定位在缓冲器内
                this.pos = (int) (desired - start);
            } else {
                // 无效的缓冲
                this.count = 0;
                this.pos = 0;
                long delta = desired % bytesPerSum;
                // 如果有的话，寻找最后一个校验点
                ((PositionCache) in).seek(desired - delta);
                // 扫描到所需位置
                for (int i = 0; i < delta; i++) {
                    read();
                }
            }

        }

        public long getPos() throws IOException {
            // 调整缓冲区
            return ((PositionCache) in).getPos() - (this.count - this.pos);
        }

        // read()的优化版本
        @Override
        public int read() throws IOException {
            if (pos >= count) {
                return super.read();
            }
            return buf[pos++] & 0xff;
        }

    }


    public FSDataInputStream(FileSystem fs, File file, int bufferSize, Configuration conf) throws IOException {
        super(null);
        this.in = new Buffer(new PositionCache(new Checker(fs, file, conf)), bufferSize);
    }


    public FSDataInputStream(FileSystem fs, File file, Configuration conf) throws IOException {
        super(null);
        int bufferSize = conf.getInt(ConfigConstants.IO_FILE_BUFFER_SIZE, ConfigConstants.IO_FILE_BUFFER_SIZE_DEFAULT);
        this.in = new Buffer(new PositionCache(new Checker(fs, file, conf)), bufferSize);
    }

    /**
     * 没有校验的构造器。
     */
    public FSDataInputStream(FSInputStream in, Configuration conf) throws IOException {
        this(in, conf.getInt(ConfigConstants.IO_FILE_BUFFER_SIZE, ConfigConstants.IO_FILE_BUFFER_SIZE_DEFAULT));
    }

    /**
     * 没有校验的构造器。
     */
    public FSDataInputStream(FSInputStream in, int bufferSize) {
        super(null);
        this.in = new Buffer(new PositionCache(in), bufferSize);
    }

    public void seek(long desired) throws IOException {
        ((Buffer) in).seek(desired);
    }

    public long getPos() throws IOException {
        return ((Buffer) in).getPos();
    }

}
