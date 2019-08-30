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

/**
 * 一组基于文件的键。
 * @author 章云
 * @date 2019/8/30 9:53
 */
public class SetFile extends MapFile {

    protected SetFile() {
    }

    /**
     * 编写一个新的集合文件。
     */
    public static class Writer extends MapFile.Writer {

        /**
         * 为命名类的键创建命名集。
         */
        public Writer(FileSystem fs, String dirName, Class keyClass) throws IOException {
            super(fs, dirName, keyClass, NullWritable.class);
        }

        /**
         * 使用命名键比较器创建命名集。
         */
        public Writer(FileSystem fs, String dirName, WritableComparator comparator) throws IOException {
            super(fs, dirName, comparator, NullWritable.class);
        }

        /**
         * 向集合追加一个键。<br/>
         * 密钥必须严格大于添加到集合中的前一个密钥。
         */
        public void append(WritableComparable key) throws IOException {
            append(key, NullWritable.get());
        }
    }

    /**
     * 提供对现有集文件的访问。
     */
    public static class Reader extends MapFile.Reader {

        /**
         * 为指定的集合构造一个集合读取器。
         */
        public Reader(FileSystem fs, String dirName, Configuration conf) throws IOException {
            super(fs, dirName, conf);
        }

        /**
         * 使用命名比较器为命名集构造一个集合读取器。
         */
        public Reader(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf) throws IOException {
            super(fs, dirName, comparator, conf);
        }

        @Override
        public boolean seek(WritableComparable key)
                throws IOException {
            return super.seek(key);
        }

        /**
         * 将集合中的下一个键读入key。<br/>
         * 如果存在这样一个键，则返回true;如果在集合结束时返回false。
         */
        public boolean next(WritableComparable key) throws IOException {
            return next(key, NullWritable.get());
        }

        /**
         * 将匹配的密钥从一个集合读入key。<br/>
         * 返回key，如果不存在匹配，则返回null。
         */
        public WritableComparable get(WritableComparable key) throws IOException {
            if (seek(key)) {
                next(key);
                return key;
            } else {
                return null;
            }
        }
    }

}
