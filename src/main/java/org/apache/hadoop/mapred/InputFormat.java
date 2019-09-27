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
 * 输入数据格式。
 * 输入文件存储在{@link FileSystem}中。
 * 输入文件的处理可以跨多台机器进行。
 * 文件作为记录序列处理，实现{@link RecordReader}。
 * 因此，文件必须根据记录边界进行分割。
 * @author 章云
 * @date 2019/9/27 8:44
 */
public interface InputFormat {

    /**
     * 分割一组输入文件。每个map任务创建一个拆分。
     * @param fs        包含要分割的文件的文件系统
     * @param job       要分割输入文件的作业
     * @param numSplits 所需的分割次数
     * @return the splits
     */
    FileSplit[] getSplits(FileSystem fs, JobConf job, int numSplits) throws IOException;

    /**
     * 为{@link FileSplit}构造一个{@link RecordReader}。
     * @param fs    the {@link FileSystem}
     * @param split the {@link FileSplit}
     * @param job   这个分块所属的工作
     * @return a {@link RecordReader}
     */
    RecordReader getRecordReader(FileSystem fs, FileSplit split, JobConf job, Reporter reporter) throws IOException;
}

