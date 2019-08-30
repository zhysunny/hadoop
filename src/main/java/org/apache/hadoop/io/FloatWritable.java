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
 * 包含类实例的浮点的可写程序。
 * @author 章云
 * @date 2019/8/30 9:33
 */
public class FloatWritable implements WritableComparable {

    static {
        WritableComparator.define(FloatWritable.class, new Comparator());
    }

    private float value;

    public FloatWritable() {
    }

    public FloatWritable(float value) {
        set(value);
    }

    public void set(float value) {
        this.value = value;
    }

    public float get() {
        return value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = in.readFloat();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(value);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FloatWritable)) {
            return false;
        }
        FloatWritable other = (FloatWritable) o;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return Float.floatToIntBits(value);
    }

    @Override
    public int compareTo(Object o) {
        float thisValue = this.value;
        float thatValue = ((FloatWritable) o).value;
        return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }

    @Override
    public String toString() {
        return Float.toString(value);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(FloatWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            float thisValue = readFloat(b1, s1);
            float thatValue = readFloat(b2, s2);
            return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
        }
    }

}

