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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * 可用作键或值的字节序列。<br/>
 * 它是可调整的，并且可以区分seqeunce的大小和当前容量。<br/>
 * 哈希函数位于缓冲区的md5的前面。<br/>
 * 排序顺序与memcmp相同。
 * @author 章云
 * @date 2019/8/30 9:28
 */
public class BytesWritable implements WritableComparable {

    static {
        WritableComparator.define(BytesWritable.class, new Comparator());
    }

    private int size;
    private byte[] bytes;

    /**
     * 创建一个零大小的序列。
     */
    public BytesWritable() {
        size = 0;
        bytes = new byte[100];
    }

    /**
     * 使用字节数组作为初始值创建一个BytesWritable。
     * @param bytes 这个数组成为对象的后备存储器。
     */
    public BytesWritable(byte[] bytes) {
        this.bytes = bytes;
        this.size = bytes.length;
    }

    /**
     * 从BytesWritable获取数据。
     * @return 数据只在0和getSize() - 1之间有效。
     */
    public byte[] get() {
        return bytes;
    }

    /**
     * 获取缓冲区的当前大小。
     * @return
     */
    public int getSize() {
        return size;
    }

    /**
     * 更改缓冲区的大小。<br/>
     * 旧范围中的值被保留，任何新值都是未定义的。<br/>
     * 如有必要，容量会改变。
     * @param size 新的字节数
     */
    public void setSize(int size) {
        if (size > getCapacity()) {
            setCapacity(size * 3 / 2);
        }
        this.size = size;
    }

    /**
     * 获取容量，这是在不调整备份存储大小的情况下可以处理的最大大小。
     * @return 字节数
     */
    public int getCapacity() {
        return bytes.length;
    }

    /**
     * 更改后备存储器的容量。<br/>
     * 数据被保存。
     * @param newCap 以字节为单位的新容量。
     */
    public void setCapacity(int newCap) {
        if (newCap != getCapacity()) {
            byte[] newData = new byte[newCap];
            if (newCap < size) {
                size = newCap;
            }
            if (size != 0) {
                System.arraycopy(bytes, 0, newData, 0, size);
            }
            bytes = newData;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // 清除旧数据
        setSize(0);
        setSize(in.readInt());
        in.readFully(bytes, 0, size);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size);
        out.write(bytes, 0, size);
    }

    @Override
    public int hashCode() {
        return WritableComparator.hashBytes(bytes, size);
    }

    /**
     * 定义BytesWritable的排序顺序。
     * @param rightObj 其他字节是可写的
     * @return 左边大于右边为正，相等时为0，左边小于右边为负。
     */
    @Override
    public int compareTo(Object rightObj) {
        BytesWritable right = ((BytesWritable) rightObj);
        return WritableComparator.compareBytes(bytes, 0, size, right.bytes, 0, right.size);
    }

    /**
     * 这两个字节序列相等吗?
     */
    @Override
    public boolean equals(Object right_obj) {
        if (right_obj instanceof BytesWritable) {
            return compareTo(right_obj) == 0;
        }
        return false;
    }

    /**
     * 为BytesWritable优化的比较器。
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(BytesWritable.class);
        }

        /**
         * 比较序列化形式的缓冲区。
         */
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int size1 = readInt(b1, s1);
            int size2 = readInt(b2, s2);
            return compareBytes(b1, s1 + 4, size1, b2, s2 + 4, size2);
        }
    }

}
