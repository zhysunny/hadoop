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
 * 从整数到值的密集基于文件的映射。
 * @author 章云
 * @date 2019/8/30 9:17
 */
public class ArrayFile extends MapFile {

    protected ArrayFile() {
    }

    /**
     * 编写一个新的数组文件。
     */
    public static class Writer extends MapFile.Writer {
        private LongWritable count = new LongWritable(0);

        /**
         * 为命名类的值创建命名文件。
         */
        public Writer(FileSystem fs, String file, Class valClass) throws IOException {
            super(fs, file, LongWritable.class, valClass);
        }

        /**
         * 向文件追加一个值。
         */
        public synchronized void append(Writable value) throws IOException {
            super.append(count, value);
            count.set(count.get() + 1);
        }
    }

    /**
     * 提供对现有数组文件的访问。
     */
    public static class Reader extends MapFile.Reader {
        private LongWritable key = new LongWritable();

        /**
         * 为指定文件构造一个数组读取器。
         */
        public Reader(FileSystem fs, String file, Configuration conf) throws IOException {
            super(fs, file, conf);
        }

        public synchronized void seek(long n) throws IOException {
            key.set(n);
            seek(key);
        }

        /**
         * 读取并返回文件中的下一个值。
         */
        public synchronized Writable next(Writable value) throws IOException {
            return next(key, value) ? value : null;
        }

        /**
         * 返回与最近一次调用{@link #seek(long)}、{@link #next(Writable)}或{@link #get(long, Writable)}相关联的键。
         */
        public synchronized long key() {
            return key.get();
        }

        public synchronized Writable get(long n, Writable value) throws IOException {
            key.set(n);
            return get(key, value);
        }
    }

}
