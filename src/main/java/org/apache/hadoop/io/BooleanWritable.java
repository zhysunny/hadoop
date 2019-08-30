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
 * 包含类实例的布尔的可写程序。
 * @author 章云
 * @date 2019/8/30 9:29
 */
public class BooleanWritable implements WritableComparable {

    static {
        WritableComparator.define(BooleanWritable.class, new Comparator());
    }

    private boolean value;

    public BooleanWritable() {
    }

    public BooleanWritable(boolean value) {
        set(value);
    }

    /**
     * 设置BooleanWritable的值
     */
    public void set(boolean value) {
        this.value = value;
    }

    /**
     * 返回BooleanWritable的值
     */
    public boolean get() {
        return value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = in.readBoolean();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(value);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BooleanWritable)) {
            return false;
        }
        BooleanWritable other = (BooleanWritable) o;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return value ? 0 : 1;
    }


    @Override
    public int compareTo(Object o) {
        boolean a = this.value;
        boolean b = ((BooleanWritable) o).value;
        return ((a == b) ? 0 : (a == false) ? -1 : 1);
    }

    /**
     * 一个比较优化布尔可写。
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(BooleanWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            boolean a = (readInt(b1, s1) == 1) ? true : false;
            boolean b = (readInt(b2, s2) == 1) ? true : false;
            return ((a == b) ? 0 : (a == false) ? -1 : 1);
        }
    }

}
