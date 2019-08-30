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
 * 包含类实例的长整型的可写程序。
 * @author 章云
 * @date 2019/8/30 9:34
 */
public class LongWritable implements WritableComparable {

    static {
        WritableComparator.define(LongWritable.class, new Comparator());
    }

    private long value;

    public LongWritable() {
    }

    public LongWritable(long value) {
        set(value);
    }

    public void set(long value) {
        this.value = value;
    }

    public long get() {
        return value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(value);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LongWritable)) {
            return false;
        }
        LongWritable other = (LongWritable) o;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return (int) value;
    }

    @Override
    public int compareTo(Object o) {
        long thisValue = this.value;
        long thatValue = ((LongWritable) o).value;
        return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(LongWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            long thisValue = readLong(b1, s1);
            long thatValue = readLong(b2, s2);
            return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
        }
    }

    public static class DecreasingComparator extends Comparator {
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

}

