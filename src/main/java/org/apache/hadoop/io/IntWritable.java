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
 * 包含类实例的整型的可写程序。
 * @author 章云
 * @date 2019/8/30 9:33
 */
public class IntWritable implements WritableComparable {

    static {
        WritableComparator.define(IntWritable.class, new Comparator());
    }

    private int value;

    public IntWritable() {
    }

    public IntWritable(int value) {
        set(value);
    }

    public void set(int value) {
        this.value = value;
    }

    public int get() {
        return value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(value);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IntWritable)) {
            return false;
        }
        IntWritable other = (IntWritable) o;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return (int) value;
    }

    @Override
    public int compareTo(Object o) {
        int thisValue = this.value;
        int thatValue = ((IntWritable) o).value;
        return thisValue - thatValue;
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);
            return thisValue - thatValue;
        }
    }

}

