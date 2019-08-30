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
import java.lang.reflect.Array;

/**
 * 包含类实例的数组的可写程序。
 * @author 章云
 * @date 2019/8/30 9:20
 */
public class ArrayWritable implements Writable {
    private Class valueClass;
    private Writable[] values;

    public ArrayWritable() {
        this.valueClass = null;
    }

    public ArrayWritable(Class valueClass) {
        this.valueClass = valueClass;
    }

    public ArrayWritable(Class valueClass, Writable[] values) {
        this(valueClass);
        this.values = values;
    }

    public ArrayWritable(String[] strings) {
        this(UTF8.class, new Writable[strings.length]);
        for (int i = 0; i < strings.length; i++) {
            values[i] = new UTF8(strings[i]);
        }
    }

    public void setValueClass(Class valueClass) {
        if (valueClass != this.valueClass) {
            this.valueClass = valueClass;
            this.values = null;
        }
    }

    public Class getValueClass() {
        return valueClass;
    }

    public String[] toStrings() {
        String[] strings = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            strings[i] = values[i].toString();
        }
        return strings;
    }

    public Object toArray() {
        Object result = Array.newInstance(valueClass, values.length);
        for (int i = 0; i < values.length; i++) {
            Array.set(result, i, values[i]);
        }
        return result;
    }

    public void set(Writable[] values) {
        this.values = values;
    }

    public Writable[] get() {
        return values;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        values = new Writable[in.readInt()];
        for (int i = 0; i < values.length; i++) {
            Writable value = WritableFactories.newInstance(valueClass);
            value.readFields(in);
            values[i] = value;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for (int i = 0; i < values.length; i++) {
            values[i].write(out);
        }
    }

}

