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

package org.apache.hadoop.conf;

/**
 * 可以配置为{@link Configuration}的东西.
 * @author 章云
 * @date 2019/7/30 8:54
 */
public interface Configurable {

    /**
     * 设置此对象要使用的配置。
     * @param conf
     */
    void setConf(Configuration conf);

    /**
     * 返回此对象使用的配置。
     * @return
     */
    Configuration getConf();
}
