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

/**
 * 一个用true调用{@link Thread#setDaemon(boolean) }的线程。
 * @author 章云
 * @date 2019/7/30 22:20
 */
public class Daemon extends Thread {

    {
        // 守护进程
        setDaemon(true);
    }

    Runnable runnable = null;

    /**
     * Construct a daemon thread.
     */
    public Daemon() {
        super();
    }

    /**
     * Construct a daemon thread.
     */
    public Daemon(Runnable runnable) {
        super(runnable);
        this.runnable = runnable;
        this.setName(runnable.toString());
    }

    public Runnable getRunnable() {
        return runnable;
    }
}
