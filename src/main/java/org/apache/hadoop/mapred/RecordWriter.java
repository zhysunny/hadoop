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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.DataOutput;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

/**
 * 将键/值对写入输出文件。
 * 由{@link OutputFormat}实现。
 * @author 章云
 * @date 2019/9/27 8:56
 */
public interface RecordWriter {
    /**
     * 写入键/值对。
     * @param key   写入的key
     * @param value 写入的value
     * @throws IOException
     */
    void write(WritableComparable key, Writable value) throws IOException;

    /**
     * 关闭。
     * @param reporter
     * @throws IOException
     */
    void close(Reporter reporter) throws IOException;
}
