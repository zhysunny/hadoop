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

import org.apache.hadoop.fs.FileSystem;

/**
 * 一种输出数据格式。输出文件存储在{@link FileSystem}中。
 * @author 章云
 * @date 2019/9/27 8:48
 */
public interface OutputFormat {

    /**
     * 构造一个{@link RecordWriter}。
     * @param fs   要写入的文件系统
     * @param job  正在编写输出的作业
     * @param name 输出的这一部分的唯一名称
     * @return a {@link RecordWriter}
     * @throws IOException
     */
    RecordWriter getRecordWriter(FileSystem fs, JobConf job, String name) throws IOException;

    /**
     * 检查作业的输出规范是否合适。
     * 提交作业时调用。
     * 通常检查它是否已经存在，在它已经存在时抛出异常，这样输出就不会被覆盖。
     * @param fs  要写入的文件系统
     * @param job 将写入其输出的作业
     * @throws IOException 当不应该尝试输出时
     */
    void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException;
}

