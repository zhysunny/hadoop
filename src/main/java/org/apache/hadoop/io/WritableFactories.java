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

import java.util.HashMap;

/**
 * 非公共可写的工厂。
 * 定义工厂许可证{@link ObjectWritable}以能够构造非公共类的实例。
 * @author 章云
 * @date 2019/8/30 9:51
 */
public class WritableFactories {
    private static final HashMap CLASS_TO_FACTORY = new HashMap();

    private WritableFactories() {
    }

    /**
     * 为类定义一个工厂。
     * @param c
     * @param factory
     */
    public static synchronized void setFactory(Class c, WritableFactory factory) {
        CLASS_TO_FACTORY.put(c, factory);
    }

    /**
     * 为类定义一个工厂。
     */
    public static synchronized WritableFactory getFactory(Class c) {
        return (WritableFactory) CLASS_TO_FACTORY.get(c);
    }

    /**
     * 使用已定义的工厂创建类的新实例。
     */
    public static Writable newInstance(Class c) {
        WritableFactory factory = WritableFactories.getFactory(c);
        if (factory != null) {
            return factory.newInstance();
        } else {
            try {
                return (Writable) c.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

}

