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

import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 将输入键/值对映射到一组中间键/值对。
 * 与给定输出键关联的所有中间值随后由map/reduce系统进行分组，并传递给{@link减速机}以确定最终的输出。
 * @author 章云
 * @date 2019/9/27 8:58
 */
public interface Mapper extends JobConfigurable, Closeable {
    /**
     * 将单个输入键/值对映射到中间键/值对。
     * 输出对不必与输入对具有相同的类型。
     * 给定的输入对可以映射为零对或多个输出对。
     * 通过调用{@link OutputCollector#collect(WritableComparable, Writable)}来收集输出对。
     * @param key
     * @param value
     * @param output   收集映射的键和值
     * @param reporter
     * @throws IOException
     */
    void map(WritableComparable key, Writable value, OutputCollector output, Reporter reporter) throws IOException;
}
