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
 * 一种可写的二维数组，包含类的实例矩阵。
 * @author 章云
 * @date 2019/8/30 9:46
 */
public class TwoDArrayWritable implements Writable {
    private Class valueClass;
    private Writable[][] values;

    public TwoDArrayWritable(Class valueClass) {
        this.valueClass = valueClass;
    }

    public TwoDArrayWritable(Class valueClass, Writable[][] values) {
        this(valueClass);
        this.values = values;
    }

    public Object toArray() {
        int[] dimensions = {values.length, 0};
        Object result = Array.newInstance(valueClass, dimensions);
        for (int i = 0; i < values.length; i++) {
            Object resultRow = Array.newInstance(valueClass, values[i].length);
            Array.set(result, i, resultRow);
            for (int j = 0; j < values[i].length; j++) {
                Array.set(resultRow, j, values[i][j]);
            }
        }
        return result;
    }

    public void set(Writable[][] values) {
        this.values = values;
    }

    public Writable[][] get() {
        return values;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        values = new Writable[in.readInt()][];
        for (int i = 0; i < values.length; i++) {
            values[i] = new Writable[in.readInt()];
        }
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < values[i].length; j++) {
                Writable value;
                try {
                    value = (Writable) valueClass.newInstance();
                } catch (InstantiationException e) {
                    throw new RuntimeException(e.toString());
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e.toString());
                }
                value.readFields(in);
                values[i][j] = value;
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for (int i = 0; i < values.length; i++) {
            out.writeInt(values[i].length);
        }
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < values[i].length; j++) {
                values[i][j].write(out);
            }
        }
    }
}

