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
import java.util.*;

/**
 * 基于{@link WritableComparable}实现类的比较器<br/>
 * 此基本实现使用自然顺序。要定义替代顺序，请覆盖{@link #compare(WritableComparable, WritableComparable)}。<br/>
 * 可以通过重写{@link #compare(byte[], int, int, byte[], int, int)}来优化比较密集型操作。<br/>
 * 提供静态实用程序方法来帮助优化此方法的实现。
 * @author 章云
 * @date 2019/8/1 16:06
 */
public class WritableComparator implements Comparator<WritableComparable> {

    /**
     * 注册的比较器集合
     */
    private static HashMap<Class<? extends WritableComparable>, WritableComparator> comparators = new HashMap<Class<? extends WritableComparable>, WritableComparator>();

    /**
     * 获取{@link WritableComparable}实现的比较器。
     * @param clz
     * @return
     */
    public static synchronized WritableComparator get(Class<? extends WritableComparable> clz) {
        WritableComparator comparator = comparators.get(clz);
        if (comparator == null) {
            comparator = new WritableComparator(clz);
        }
        return comparator;
    }

    /**
     * 为{@link WritableComparable}实现类注册一个优化的比较器。
     * @param clz
     * @param comparator
     */
    public static synchronized void define(Class<? extends WritableComparable> clz, WritableComparator comparator) {
        comparators.put(clz, comparator);
    }


    private DataInputBuffer buffer = new DataInputBuffer();

    private Class<? extends WritableComparable> keyClass;
    private WritableComparable key1;
    private WritableComparable key2;

    /**
     * 构造一个{@link WritableComparable}实现。
     * @param keyClass
     */
    protected WritableComparator(Class<? extends WritableComparable> keyClass) {
        this.keyClass = keyClass;
        this.key1 = newKey();
        this.key2 = newKey();
    }

    /**
     * 返回WritableComparable实现类。
     */
    public Class<? extends WritableComparable> getKeyClass() {
        return keyClass;
    }

    /**
     * 构造一个新的{@link WritableComparable}实例。
     */
    public WritableComparable newKey() {
        try {
            return keyClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 优化钩。重写此命令以生成SequenceFile。<br/>
     * 默认实现将数据读入两个{@link WritableComparable}(使用{@link Writable#readFields(DataInput)}，然后调用{@link #compare(WritableComparable, WritableComparable)}。
     * @param b1
     * @param s1
     * @param l1
     * @param b2
     * @param s2
     * @param l2
     * @return
     */
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
        try {
            // parse key1
            buffer.reset(b1, s1, l1);
            key1.readFields(buffer);
            // parse key2
            buffer.reset(b2, s2, l2);
            key2.readFields(buffer);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // compare them
        return compare(key1, key2);
    }

    /**
     * 比较两个WritableComparables。<br/>
     * 默认实现使用自然顺序，调用{@link Comparable#compareTo(Object)}。
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return a.compareTo(b);
    }

    /**
     * 二进制数据的字典顺序。
     * @param b1
     * @param s1
     * @param l1
     * @param b2
     * @param s2
     * @param l2
     * @return
     */
    public static int compareBytes(byte[] b1, int s1, int l1,
                                   byte[] b2, int s2, int l2) {
        int end1 = s1 + l1;
        int end2 = s2 + l2;
        for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
            int a = (b1[i] & 0xff);
            int b = (b2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return l1 - l2;
    }

    /**
     * 为二进制数据计算哈希。
     * @param bytes
     * @param length
     * @return
     */
    public static int hashBytes(byte[] bytes, int length) {
        int hash = 1;
        for (int i = 0; i < length; i++) {
            hash = (31 * hash) + (int) bytes[i];
        }
        return hash;
    }

    /**
     * 从字节数组解析无符号短代码。
     * @param bytes
     * @param start
     * @return
     */
    public static int readUnsignedShort(byte[] bytes, int start) {
        return (((bytes[start] & 0xff) << 8) +
                ((bytes[start + 1] & 0xff)));
    }

    /**
     * 从字节数组解析整数。
     * @param bytes
     * @param start
     * @return
     */
    public static int readInt(byte[] bytes, int start) {
        return (((bytes[start] & 0xff) << 24) +
                ((bytes[start + 1] & 0xff) << 16) +
                ((bytes[start + 2] & 0xff) << 8) +
                ((bytes[start + 3] & 0xff)));

    }

    /**
     * 从字节数组解析浮点数。
     * @param bytes
     * @param start
     * @return
     */
    public static float readFloat(byte[] bytes, int start) {
        return Float.intBitsToFloat(readInt(bytes, start));
    }

    /**
     * 从字节数组中解析长整数。
     * @param bytes
     * @param start
     * @return
     */
    public static long readLong(byte[] bytes, int start) {
        return ((long) (readInt(bytes, start)) << 32) +
                (readInt(bytes, start + 4) & 0xFFFFFFFFL);
    }

}
