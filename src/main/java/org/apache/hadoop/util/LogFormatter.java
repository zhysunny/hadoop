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

package org.apache.hadoop.util;

import java.util.logging.*;
import java.io.*;
import java.text.*;
import java.util.Date;

/**
 * 只打印日期和日志消息。
 * @author 章云
 * @date 2019/7/30 9:02
 */
@Deprecated
public class LogFormatter extends Formatter {
    private static final String FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String NEWLINE = System.getProperty("line.separator");

    private final Date date = new Date();
    private final SimpleDateFormat formatter = new SimpleDateFormat(FORMAT);

    private static boolean loggedSevere = false;
    /**
     * 是否记录每个条目的时间
     */
    private static boolean showTime = true;
    /**
     * 是否记录线程id
     */
    private static boolean showThreadIDs = false;

    static {
        // 加载类时初始化
        Handler[] handlers = LogFormatter.getLogger("").getHandlers();
        for (int i = 0; i < handlers.length; i++) {
            handlers[i].setFormatter(new LogFormatter());
            handlers[i].setLevel(Level.FINEST);
        }
    }

    /**
     * 获取一个日志程序，并作为副作用将其安装为默认格式化程序。
     * @param name 类全名
     * @return
     */
    public static Logger getLogger(String name) {
        // 只要引用这个类就会安装它
        return Logger.getLogger(name);
    }

    /**
     * 获取一个日志程序，并作为副作用将其安装为默认格式化程序。
     * @param clz 类字节码
     * @return
     */
    public static Logger getLogger(Class<?> clz) {
        // 只要引用这个类就会安装它
        return Logger.getLogger(clz.getName());
    }

    /**
     * 如果为真，则记录每个条目的时间。
     * @param showTime
     */
    public static void showTime(boolean showTime) {
        LogFormatter.showTime = showTime;
    }

    /**
     * 当设置为true时，将记录线程id。
     * @param showThreadIDs
     */
    public static void setShowThreadIDs(boolean showThreadIDs) {
        LogFormatter.showThreadIDs = showThreadIDs;
    }

    /**
     * 格式化给定的日志记录。
     * @param record 要格式化的日志记录。
     * @return 返回格式化的日志记录
     */
    @Override
    public synchronized String format(LogRecord record) {
        StringBuffer buffer = new StringBuffer();
        // the date
        if (showTime) {
            date.setTime(record.getMillis());
            formatter.format(date, buffer, new FieldPosition(0));
        }
        // the thread id
        if (showThreadIDs) {
            buffer.append(" ");
            buffer.append(record.getThreadID());
        }
        // handle SEVERE specially
        if (record.getLevel() == Level.SEVERE) {
            // flag it in log
            buffer.append(" SEVERE");
            // set global flag
            loggedSevere = true;
        }
        // the message
        buffer.append(" ");
        buffer.append(formatMessage(record));
        buffer.append(NEWLINE);
        if (record.getThrown() != null) {
            try {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                record.getThrown().printStackTrace(pw);
                pw.close();
                buffer.append(sw.toString());
            } catch (Exception ex) {
            }
        }
        return buffer.toString();
    }

    /**
     * 如果LogFormatter在Level.SEVERE记录了什么，则返回true
     * @return
     */
    public static boolean hasLoggedSevere() {
        return loggedSevere;
    }

    /**
     * 返回一个流，当写入该流时，将添加日志行。
     * @param logger
     * @param level
     * @return
     */
    public static PrintStream getLogStream(final Logger logger, final Level level) {
        return new PrintStream(new ByteArrayOutputStream() {
            private int scan = 0;

            /**
             * 字节数组是否有回车键
             * @return
             */
            private boolean hasNewline() {
                for (; scan < count; scan++) {
                    if (buf[scan] == '\n') {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void flush() throws IOException {
                if (!hasNewline()) {
                    return;
                }
                logger.log(level, toString().trim());
                reset();
                scan = 0;
            }
        }, true);
    }
}
