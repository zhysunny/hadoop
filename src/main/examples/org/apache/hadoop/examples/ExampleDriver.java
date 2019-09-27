/**
 * Copyright 2006 The Apache Software Foundation
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

package org.apache.hadoop.examples;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 样例测试主入口
 * @author 章云
 * @date 2019/9/26 11:49
 */
public class ExampleDriver {

    /**
     * 基于类和人类可读描述的示例程序的描述。
     */
    static private class ProgramDescription {

        private Method main;
        private String description;

        static final Class[] paramTypes = new Class[]{String[].class};

        /**
         * 创建示例程序的描述。
         * @param mainClass   该类以main为例程序
         * @param description 在帮助消息中显示给用户的字符串
         * @throws SecurityException     如果我们不能使用反射
         * @throws NoSuchMethodException 如果类没有主方法
         */
        public ProgramDescription(Class mainClass, String description) throws SecurityException, NoSuchMethodException {
            this.main = mainClass.getMethod("main", paramTypes);
            this.description = description;
        }

        /**
         * 使用给定的参数调用示例应用程序
         * @param args 应用程序的参数
         * @throws Throwable 被调用方法引发的异常
         */
        public void invoke(String[] args) throws Throwable {
            try {
                main.invoke(null, new Object[]{args});
            } catch (InvocationTargetException except) {
                throw except.getCause();
            }
        }

        public String getDescription() {
            return description;
        }

    }

    private static void printUsage(Map<String, ProgramDescription> programs) {
        System.out.println("Valid program names are:");
        for (Iterator<Entry<String, ProgramDescription>> itr = programs.entrySet().iterator(); itr.hasNext(); ) {
            Map.Entry<String, ProgramDescription> item = itr.next();
            System.out.println("  " + item.getKey() + ": " + item.getValue().getDescription());
        }
    }

    /**
     * 这是示例程序的驱动程序。
     * 它查看第一个命令行参数，并试图找到一个具有该名称的示例程序。
     * 如果找到它，它将使用其余的命令行参数调用该类中的main方法。
     * @param args 从用户处获取参数。args[0]是要运行的命令。
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws Throwable                由示例程序的主程序抛出的任何内容
     */
    public static void main(String[] args) throws Throwable {
        Map<String, ProgramDescription> programs = new TreeMap();

        // 将新程序添加到此列表中
        programs.put("wordcount", new ProgramDescription(WordCount.class, "A map/reduce program that counts the words in the input files."));
        programs.put("grep", new ProgramDescription(Grep.class, "A map/reduce program that counts the matches of a regex in the input."));

        // 确保他们给了我们一个程序名。
        if (args.length == 0) {
            System.out.println("An example program must be given as the first argument.");
            printUsage(programs);
            return;
        }

        ProgramDescription pgm = programs.get(args[0]);
        if (pgm == null) {
            System.out.println("Unknown program '" + args[0] + "' chosen.");
            printUsage(programs);
            return;
        }

        // 删除前导参数并调用main
        String[] new_args = new String[args.length - 1];
        for (int i = 1; i < args.length; ++i) {
            new_args[i - 1] = args[i];
        }
        pgm.invoke(new_args);
    }

}
