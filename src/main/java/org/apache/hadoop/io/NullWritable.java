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
 * 单例可写，没有数据。
 * @author 章云
 * @date 2019/8/30 9:42
 */
public class NullWritable implements Writable {

    private static final NullWritable THIS = new NullWritable();

    private NullWritable() {
    }

    public static NullWritable get() {
        return THIS;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }
}

