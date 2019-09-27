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
import java.io.DataInput;

import org.apache.hadoop.io.Writable;

/**
 * 从输入文件{@link FileSplit}中读取键/值对。
 * 由{@link InputFormat}实现。
 * @author 章云
 * @date 2019/9/27 8:53
 */
public interface RecordReader {
    /**
     * 读取下一个键/值对。
     * @param key the key to read data into
     * @param value the value to read data into
     * @return true iff a key/value was read, false if at EOF
     *
     * @see Writable#readFields(DataInput)
     */
    /**
     * 读取下一个键/值对。
     * @param key   读取数据的key
     * @param value 读取数据的value
     * @return 如果键/值被读取为true，则在EOF时为false
     * @throws IOException
     */
    boolean next(Writable key, Writable value) throws IOException;

    /**
     * 返回输入中的当前位置。
     * @return
     * @throws IOException
     */
    long getPos() throws IOException;

    /**
     * 关闭。
     * @throws IOException
     */
    void close() throws IOException;

}
