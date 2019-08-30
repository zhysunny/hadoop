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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从键到值的基于文件的映射。<br/>
 * 一个map是一个包含两个文件的目录，其中data file，包含map中的所有键和值，以及一个较小的index<br/>
 * 文件，包含部分键。<br/>
 * 分数由{@link Writer#getIndexInterval()}决定。<br/>
 * 索引文件被完全读入内存。<br/>
 * 因此，关键实现应该尽量保持自身的小。<br/>
 * 映射文件是通过按顺序添加条目创建的。<br/>
 * 要维护大型数据库，可以通过复制数据库的前一个版本并合并到已排序的更改列表中来执行更新，从而在新文件中创建数据库的新版本。<br/>
 * 可以使用{@link SequenceFile.Sorter}对大型更改列表进行排序。
 * @author 章云
 * @date 2019/8/30 8:54
 */
public class MapFile {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapFile.class);
    /**
     * 索引文件的名称。
     */
    public static final String INDEX_FILE_NAME = "index";

    /**
     * 数据文件的名称。
     */
    public static final String DATA_FILE_NAME = "data";

    protected MapFile() {
    }

    /**
     * Writes a new map.
     */
    public static class Writer {
        private SequenceFile.Writer data;
        private SequenceFile.Writer index;

        private int indexInterval = 128;

        private long size;
        private LongWritable position = new LongWritable();

        // 以下字段仅用于检查密钥顺序
        private WritableComparator comparator;
        private DataInputBuffer inBuf = new DataInputBuffer();
        private DataOutputBuffer outBuf = new DataOutputBuffer();
        private WritableComparable lastKey;


        /**
         * 为已命名类的键创建已命名映射。
         */
        public Writer(FileSystem fs, String dirName, Class keyClass, Class valClass) throws IOException {
            this(fs, dirName, WritableComparator.get(keyClass), valClass, false);
        }

        /**
         * 为已命名类的键创建已命名映射。
         */
        public Writer(FileSystem fs, String dirName, Class keyClass, Class valClass, boolean compress) throws IOException {
            this(fs, dirName, WritableComparator.get(keyClass), valClass, compress);
        }

        /**
         * 使用命名键比较器创建命名映射。
         */
        public Writer(FileSystem fs, String dirName, WritableComparator comparator, Class valClass) throws IOException {
            this(fs, dirName, comparator, valClass, false);
        }

        /**
         * 使用命名键比较器创建命名映射。
         */
        public Writer(FileSystem fs, String dirName, WritableComparator comparator, Class valClass, boolean compress) throws IOException {
            this.comparator = comparator;
            this.lastKey = comparator.newKey();

            File dir = new File(dirName);
            fs.mkdirs(dir);

            File dataFile = new File(dir, DATA_FILE_NAME);
            File indexFile = new File(dir, INDEX_FILE_NAME);

            Class keyClass = comparator.getKeyClass();
            this.data = new SequenceFile.Writer(fs, dataFile.getPath(), keyClass, valClass, compress);
            this.index = new SequenceFile.Writer(fs, indexFile.getPath(), keyClass, LongWritable.class);
        }

        /**
         * 在添加索引项之前添加的项数。
         */
        public int getIndexInterval() {
            return indexInterval;
        }

        /**
         * 设置索引间隔。
         * @see #getIndexInterval()
         */
        public void setIndexInterval(int interval) {
            indexInterval = interval;
        }

        public synchronized void close() throws IOException {
            data.close();
            index.close();
        }

        /**
         * 向映射追加一个键/值对。<br/>
         * 键必须大于或等于添加到映射中的前一个键。
         */
        public synchronized void append(WritableComparable key, Writable val) throws IOException {
            checkKey(key);
            if (size % indexInterval == 0) {
                // 添加索引项
                position.set(data.getLength());
                index.append(key, position);
            }
            // 向数据添加键/值
            data.append(key, val);
            size++;
        }

        private void checkKey(WritableComparable key) throws IOException {
            // 检查钥匙是否摆放整齐
            if (size != 0 && comparator.compare(lastKey, key) > 0) {
                throw new IOException("key out of order: " + key + " after " + lastKey);
            }
            // 通过写和读，用密钥的副本更新lastKey
            outBuf.reset();
            key.write(outBuf);

            inBuf.reset(outBuf.getData(), outBuf.getLength());
            lastKey.readFields(inBuf);
        }

    }

    /**
     * 提供对现有映射的访问。
     */
    public static class Reader {

        /**
         * 要在每个条目之间跳过的索引条目的数量。<br/>
         * 默认为零。<br/>
         * 将此值设置为大于零的值可以方便地使用更少的内存打开大型映射文件。
         */
        private int INDEX_SKIP = 0;

        private WritableComparator comparator;

        private DataOutputBuffer keyBuf = new DataOutputBuffer();
        private DataOutputBuffer nextBuf = new DataOutputBuffer();
        private int nextKeyLen = -1;
        private long seekPosition = -1;
        private int seekIndex = -1;
        private long firstPosition;

        private WritableComparable getKey;

        // 数据在磁盘上
        private SequenceFile.Reader data;
        private SequenceFile.Reader index;

        // 索引阅读器是否已关闭
        private boolean indexClosed = false;

        // 索引在内存中
        private int count = -1;
        private WritableComparable[] keys;
        private long[] positions;

        /**
         * 返回此文件中的键的类。
         */
        public Class getKeyClass() {
            return data.getKeyClass();
        }

        /**
         * 返回此文件中的值的类。
         */
        public Class getValueClass() {
            return data.getValueClass();
        }

        /**
         * 为指定的map构造一个map阅读器。
         */
        public Reader(FileSystem fs, String dirName, Configuration conf) throws IOException {
            this(fs, dirName, null, conf);
            INDEX_SKIP = Constants.IO_MAP_INDEX_SKIP;
        }

        /**
         * 使用命名比较器为命名映射构造一个map阅读器。
         */
        public Reader(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf) throws IOException {
            File dir = new File(dirName);
            File dataFile = new File(dir, DATA_FILE_NAME);
            File indexFile = new File(dir, INDEX_FILE_NAME);
            this.data = new SequenceFile.Reader(fs, dataFile.getPath(), conf);
            this.firstPosition = data.getPosition();
            if (comparator == null) {
                this.comparator = WritableComparator.get(data.getKeyClass());
            } else {
                this.comparator = comparator;
            }
            this.getKey = this.comparator.newKey();
            this.index = new SequenceFile.Reader(fs, indexFile.getPath(), conf);
        }

        private void readIndex() throws IOException {
            // 将索引完全读入内存
            if (this.keys != null) {
                return;
            }
            this.count = 0;
            this.keys = new WritableComparable[1024];
            this.positions = new long[1024];
            try {
                int skip = INDEX_SKIP;
                LongWritable position = new LongWritable();
                WritableComparable lastKey = null;
                while (true) {
                    WritableComparable k = comparator.newKey();
                    if (!index.next(k, position)) {
                        break;
                    }
                    // 检查顺序以确保比较器是兼容的
                    if (lastKey != null && comparator.compare(lastKey, k) > 0) {
                        throw new IOException("key out of order: " + k + " after " + lastKey);
                    }
                    lastKey = k;
                    if (skip > 0) {
                        skip--;
                        // 跳过这个条目
                        continue;
                    } else {
                        // 重置跳过
                        skip = INDEX_SKIP;
                    }
                    // 数组生长时间
                    if (count == keys.length) {
                        int newLength = (keys.length * 3) / 2;
                        WritableComparable[] newKeys = new WritableComparable[newLength];
                        long[] newPositions = new long[newLength];
                        System.arraycopy(keys, 0, newKeys, 0, count);
                        System.arraycopy(positions, 0, newPositions, 0, count);
                        keys = newKeys;
                        positions = newPositions;
                    }
                    keys[count] = k;
                    positions[count] = position.get();
                    count++;
                }
            } catch (EOFException e) {
                LOGGER.warn("Unexpected EOF reading " + index + " at entry #" + count + ".  Ignoring.");
            } finally {
                indexClosed = true;
                index.close();
            }
        }

        /**
         * 在第一个键之前重新定位阅读器。
         */
        public synchronized void reset() throws IOException {
            data.seek(firstPosition);
        }

        /**
         * 从文件中读取最后一个键。
         * @param key 读入键
         */
        public synchronized void finalKey(WritableComparable key) throws IOException {
            // 保存的位置
            long originalPosition = data.getPosition();
            try {
                // 确保索引是有效的
                readIndex();
                if (count > 0) {
                    // 跳转到最后一个索引项
                    data.seek(positions[count - 1]);
                } else {
                    // 从头开始
                    reset();
                }
                while (data.next(key)) {
                }
            } finally {
                // 恢复的位置
                data.seek(originalPosition);
            }
        }

        /**
         * 将读取器定位在命名键处，如果不存在，则定位在命名键后的第一个条目处。<br/>
         * 如果指定的键存在于此映射中，则返回true。
         */
        public synchronized boolean seek(WritableComparable key) throws IOException {
            // 确保索引已被读取
            readIndex();
            // 将key写入keyBuf
            keyBuf.reset();
            key.write(keyBuf);
            // 应用前
            if (seekIndex != -1  // 应用前
                    && seekIndex + 1 < count
                    && comparator.compare(key, keys[seekIndex + 1]) < 0 // 下一个索引之前
                    && comparator.compare(keyBuf.getData(), 0, keyBuf.getLength(),
                    nextBuf.getData(), 0, nextKeyLen)
                    >= 0) {                                 // 但在最后一次寻找之后
                // 什么都不做
            } else {
                seekIndex = binarySearch(key);
                if (seekIndex < 0) {
                    seekIndex = -seekIndex - 2;
                }
                if (seekIndex == -1) {
                    seekPosition = firstPosition; // 使用文件开头
                } else {
                    seekPosition = positions[seekIndex]; // 其他使用索引
                }
            }
            data.seek(seekPosition);
            while ((nextKeyLen = data.next(nextBuf.reset())) != -1) {
                int c = comparator.compare(keyBuf.getData(), 0, keyBuf.getLength(), nextBuf.getData(), 0, nextKeyLen);
                if (c <= 0) {  // 达到或超过期望
                    data.seek(seekPosition);  // 退回到以前
                    return c == 0;
                }
                seekPosition = data.getPosition();
            }

            return false;
        }

        private int binarySearch(WritableComparable key) {
            int low = 0;
            int high = count - 1;
            while (low <= high) {
                int mid = (low + high) >> 1;
                WritableComparable midVal = keys[mid];
                int cmp = comparator.compare(midVal, key);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return mid; // 找到key
                }
            }
            return -(low + 1); // 没找到key
        }

        /**
         * 将映射中的下一对键/值读入key和val。<br/>
         * 如果存在这样一对，返回true;如果在映射的末尾返回false
         */
        public synchronized boolean next(WritableComparable key, Writable val) throws IOException {
            return data.next(key, val);
        }

        /**
         * 返回指定键的值，如果不存在，则返回null。
         */
        public synchronized Writable get(WritableComparable key, Writable val) throws IOException {
            if (seek(key)) {
                next(getKey, val);
                return val;
            } else {
                return null;
            }
        }

        public synchronized void close() throws IOException {
            if (!indexClosed) {
                index.close();
            }
            data.close();
        }

    }

    /**
     * 重命名现有map目录。
     */
    public static void rename(FileSystem fs, String oldName, String newName) throws IOException {
        File oldDir = new File(oldName);
        File newDir = new File(newName);
        if (!fs.rename(oldDir, newDir)) {
            throw new IOException("Could not rename " + oldDir + " to " + newDir);
        }
    }

    /**
     * 删除指定的映射文件。
     */
    public static void delete(FileSystem fs, String name) throws IOException {
        File dir = new File(name);
        File data = new File(dir, DATA_FILE_NAME);
        File index = new File(dir, INDEX_FILE_NAME);

        fs.delete(data);
        fs.delete(index);
        fs.delete(dir);
    }

    /**
     * 此方法试图通过重新创建索引来修复损坏的映射文件。
     * @param fs         文件系统
     * @param dir        包含映射文件数据和索引的目录
     * @param keyClass   key类(必须是Writable的子类)
     * @param valueClass value类(必须是Writable的子类)
     * @param dryrun     不执行任何更改，只报告需要做什么
     * @return 此映射文件中有效条目的数量，如果不需要修复，则为-1
     * @throws Exception
     */
    public static long fix(FileSystem fs, File dir, Class keyClass, Class valueClass, boolean dryrun, Configuration conf) throws Exception {
        String dr = (dryrun ? "[DRY RUN ] " : "");
        File data = new File(dir, DATA_FILE_NAME);
        File index = new File(dir, INDEX_FILE_NAME);
        int indexInterval = 128;
        if (!fs.exists(data)) {
            // 我们无能为力!
            throw new Exception(dr + "Missing data file in " + dir + ", impossible to fix this.");
        }
        if (fs.exists(index)) {
            // 不需要修复
            return -1;
        }
        SequenceFile.Reader dataReader = new SequenceFile.Reader(fs, data.toString(), conf);
        if (!dataReader.getKeyClass().equals(keyClass)) {
            throw new Exception(dr + "Wrong key class in " + dir + ", expected" + keyClass.getName() + ", got " + dataReader.getKeyClass().getName());
        }
        if (!dataReader.getValueClass().equals(valueClass)) {
            throw new Exception(dr + "Wrong value class in " + dir + ", expected" + valueClass.getName() + ", got " + dataReader.getValueClass().getName());
        }
        long cnt = 0L;
        Writable key = (Writable) keyClass.getConstructor(new Class[0]).newInstance(new Object[0]);
        Writable value = (Writable) valueClass.getConstructor(new Class[0]).newInstance(new Object[0]);
        SequenceFile.Writer indexWriter = null;
        if (!dryrun) {
            indexWriter = new SequenceFile.Writer(fs, index.toString(), keyClass, LongWritable.class);
        }
        try {
            long pos = 0L;
            LongWritable position = new LongWritable();
            while (dataReader.next(key, value)) {
                cnt++;
                if (cnt % indexInterval == 0) {
                    position.set(pos);
                    if (!dryrun) {
                        indexWriter.append(key, position);
                    }
                }
                pos = dataReader.getPosition();
            }
        } catch (Throwable t) {
        }
        dataReader.close();
        if (!dryrun) {
            indexWriter.close();
        }
        return cnt;
    }


    public static void main(String[] args) throws Exception {
        String usage = "Usage: MapFile inFile outFile";
        if (args.length != 2) {
            System.err.println(usage);
            System.exit(-1);
        }
        String in = args[0];
        String out = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = new LocalFileSystem(conf);
        MapFile.Reader reader = new MapFile.Reader(fs, in, conf);
        MapFile.Writer writer = new MapFile.Writer(fs, out, reader.getKeyClass(), reader.getValueClass());
        WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
        Writable value = (Writable) reader.getValueClass().newInstance();
        while (reader.next(key, value)) {
            writer.append(key, value);
        }
        writer.close();
    }

}
