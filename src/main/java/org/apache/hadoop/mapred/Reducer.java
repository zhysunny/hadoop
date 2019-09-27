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

import java.util.Iterator;

import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 减少共享较小值集的键的一组中间值。
 * 输入值是{@link Mapper}的分组输出。
 * @author 章云
 * @date 2019/9/27 9:00
 */
public interface Reducer extends JobConfigurable, Closeable {
    /**
     * 组合给定键的值。
     * 输出值必须与输入值具有相同的类型。
     * 输入键不能改变。
     * 通常，所有值都被组合成零或一个值。
     * 通过调用{@link OutputCollector#collect(WritableComparable, Writable)}来收集输出对。
     * @param key
     * @param values
     * @param output   收集组合值
     * @param reporter
     * @throws IOException
     */
    void reduce(WritableComparable key, Iterator values, OutputCollector output, Reporter reporter) throws IOException;

}
