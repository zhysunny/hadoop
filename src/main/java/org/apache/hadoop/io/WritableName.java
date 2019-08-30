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
import java.io.IOException;

/**
 * 实用程序，允许重命名可写实现类，而无需调用包含类名的文件。
 * @author 章云
 * @date 2019/8/30 9:52
 */
public class WritableName {
    private static HashMap NAME_TO_CLASS = new HashMap();
    private static HashMap CLASS_TO_NAME = new HashMap();

    static {
        WritableName.setName(NullWritable.class, "null");
        WritableName.setName(LongWritable.class, "long");
        WritableName.setName(UTF8.class, "UTF8");
        WritableName.setName(MD5Hash.class, "MD5Hash");
    }

    private WritableName() {
    }

    /**
     * 将类的名称设置为类名之外的名称。
     */
    public static synchronized void setName(Class writableClass, String name) {
        CLASS_TO_NAME.put(writableClass, name);
        NAME_TO_CLASS.put(name, writableClass);
    }

    /**
     * 为类添加替代名称。
     */
    public static synchronized void addName(Class writableClass, String name) {
        NAME_TO_CLASS.put(name, writableClass);
    }

    /**
     * 返回类的名称。默认值是{@link Class#getName()}。
     */
    public static synchronized String getName(Class writableClass) {
        String name = (String) CLASS_TO_NAME.get(writableClass);
        if (name != null) {
            return name;
        }
        return writableClass.getName();
    }

    /**
     * 返回类的名称。默认值是{@link Class#forName(String)}。
     */
    public static synchronized Class getClass(String name) throws IOException {
        Class writableClass = (Class) NAME_TO_CLASS.get(name);
        if (writableClass != null) {
            return writableClass;
        }
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new IOException(e.toString());
        }
    }

}
