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

package org.apache.hadoop.ipc;

import java.io.IOException;
import java.io.EOFException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.StringWriter;
import java.io.PrintWriter;

import java.net.Socket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import java.util.LinkedList;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个抽象的IPC服务。
 * IPC调用接受单个{@link Writable}作为参数，并返回一个{@link Writable}作为它们的值。
 * 服务在端口上运行，由参数类和值类定义。
 * @author 章云
 * @date 2019/8/13 9:07
 */
public abstract class Server {
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

    /**
     * 返回在or null下调用的服务器实例。
     * 可以在{@link #call(Writable)}实现下调用，也可以在参数和返回值的{@link Writable}方法下调用。
     * 允许应用程序访问服务器上下文。
     */
    public static Server get() {
        return SERVER.get();
    }

    /**
     * namenode端口
     */
    private int port;
    /**
     * 处理器线程数
     */
    private int handlerCount;
    /**
     * 队列调用最大数
     */
    private int maxQueuedCalls;
    /**
     * 调用参数类
     */
    private Class paramClass;
    /**
     * 配置类
     */
    private Configuration conf;
    /**
     * 超时时间，ipc.client.timeout配置项，默认60秒
     */
    private int timeout;

    /**
     * 服务器运行时为true
     */
    private boolean running = true;
    /**
     * 调用类队列
     */
    private LinkedList<Call> callQueue = new LinkedList<Call>();
    /**
     * used by wait/notify
     */
    private Object callDequeued = new Object();

    /**
     * 排队等待处理的调用。
     */
    private static class Call {
        /**
         * 客户端调用id
         */
        private int id;
        /**
         * 传递的参数
         */
        private Writable param;
        /**
         * 连接客户端
         */
        private Connection connection;

        public Call(int id, Writable param, Connection connection) {
            this.id = id;
            this.param = param;
            this.connection = connection;
        }
    }

    /**
     * 监听类，启动新的连接线程。<br/>
     * socket.accept()为阻塞状态，当服务端接收到信息时，启动Connection线程
     */
    private class Listener extends Thread {
        private ServerSocket socket;

        public Listener() throws IOException {
            this.socket = new ServerSocket(port);
            socket.setSoTimeout(timeout);
            this.setDaemon(true);
            this.setName("Server listener on port " + port);
        }

        @Override
        public void run() {
            LOGGER.info(getName() + ": starting");
            while (running) {
                try {
                    // start a new connection
                    new Connection(socket.accept()).start();
                } catch (SocketTimeoutException e) {
                    // ignore timeouts
                } catch (Exception e) {
                    // log all other exceptions
                    LOGGER.info(getName() + " caught: " + e, e);
                }
            }
            try {
                socket.close();
            } catch (IOException e) {
                LOGGER.info(getName() + ": e=" + e);
            }
            LOGGER.info(getName() + ": exiting");
        }
    }

    /**
     * 从连接读取调用并对其进行排队处理。
     */
    private class Connection extends Thread {
        private Socket socket;
        private DataInputStream in;
        private DataOutputStream out;

        public Connection(Socket socket) throws IOException {
            this.socket = socket;
            socket.setSoTimeout(timeout);
            // 服务端接收的输入流
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            // 服务端给客户端的输出流
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            this.setDaemon(true);
            this.setName("Server connection on port " + port + " from " + socket.getInetAddress().getHostAddress());
        }

        @Override
        public void run() {
            LOGGER.info(getName() + ": starting");
            SERVER.set(Server.this);
            try {
                while (running) {
                    int id;
                    try {
                        id = in.readInt();
                    } catch (SocketTimeoutException e) {
                        continue;
                    }
                    LOGGER.info(getName() + " got #" + id);
                    Writable param = makeParam();
                    param.readFields(in);
                    Call call = new Call(id, param, this);
                    synchronized (callQueue) {
                        callQueue.addLast(call);
                        // 唤醒等待的处理线程
                        callQueue.notify();
                    }
                    while (running && callQueue.size() >= maxQueuedCalls) {
                        synchronized (callDequeued) {         // queue is full
                            callDequeued.wait(timeout);         // wait for a dequeue
                        }
                    }
                }
            } catch (EOFException eof) {
                // 这就是在linux上，当另一端关闭时所发生的事情
            } catch (SocketException eof) {
                // 这就是在Win32上，当另一端关闭时所发生的事情
            } catch (Exception e) {
                LOGGER.error(getName() + " caught: " + e, e);
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                }
                LOGGER.error(getName() + ": exiting");
            }
        }

    }

    /**
     * 处理队列的调用。
     */
    private class Handler extends Thread {
        public Handler(int instanceNumber) {
            this.setDaemon(true);
            this.setName("Server handler " + instanceNumber + " on " + port);
        }

        @Override
        public void run() {
            LOGGER.info(getName() + ": starting");
            SERVER.set(Server.this);
            while (running) {
                try {
                    Call call;
                    synchronized (callQueue) {
                        while (running && callQueue.size() == 0) { // wait for a call
                            callQueue.wait(timeout);
                        }
                        if (!running) {
                            break;
                        }
                        call = callQueue.removeFirst(); // pop the queue
                    }

                    synchronized (callDequeued) {           // tell others we've dequeued
                        callDequeued.notify();
                    }
                    LOGGER.info(getName() + ": has #" + call.id + " from " + call.connection.socket.getInetAddress().getHostAddress());
                    String error = null;
                    Writable value = null;
                    try {
                        value = call(call.param);             // make the call
                    } catch (IOException e) {
                        LOGGER.error(getName() + " call error: " + e, e);
                        error = getStackTrace(e);
                    } catch (Exception e) {
                        LOGGER.error(getName() + " call error: " + e, e);
                        error = getStackTrace(e);
                    }

                    DataOutputStream out = call.connection.out;
                    synchronized (out) {
                        out.writeInt(call.id);                // write call id
                        out.writeBoolean(error != null);        // write error flag
                        if (error != null) {
                            value = new UTF8(error);
                        }
                        value.write(out);                     // write value
                        out.flush();
                    }

                } catch (Exception e) {
                    LOGGER.error(getName() + " caught: " + e, e);
                }
            }
            LOGGER.info(getName() + ": exiting");
        }

        private String getStackTrace(Throwable throwable) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            throwable.printStackTrace(printWriter);
            printWriter.flush();
            return stringWriter.toString();
        }

    }

    /**
     * 构造在指定端口上侦听的服务器。
     * 传递的参数必须属于指定的类。
     * handlerCount确定将用于处理调用的处理程序线程的数量。
     */
    protected Server(int port, Class paramClass, int handlerCount, Configuration conf) {
        this.conf = conf;
        this.port = port;
        this.paramClass = paramClass;
        this.handlerCount = handlerCount;
        this.maxQueuedCalls = handlerCount;
        this.timeout = Constants.IPC_CLIENT_TIMEOUT;
    }

    /**
     * 设置用于网络i/o的超时。
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * 启动服务。必须在处理任何调用之前调用。
     */
    public synchronized void start() throws IOException {
        Listener listener = new Listener();
        listener.start();

        for (int i = 0; i < handlerCount; i++) {
            Handler handler = new Handler(i);
            handler.start();
        }
    }

    /**
     * 停止服务。
     * 调用此函数后，将不处理任何新调用。
     * 所有子线程都可能在这个返回之后完成。
     */
    public synchronized void stop() {
        LOGGER.info("Stopping server on " + port);
        running = false;
        try {
            // 等待请求完成
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
        }
        notifyAll();
    }

    /**
     * 等待服务器停止。
     * 不等待所有子线程完成。
     * 看到{@link #stop()}。
     */
    public synchronized void join() throws InterruptedException {
        while (running) {
            wait();
        }
    }

    /**
     * Called for each call.
     */
    public abstract Writable call(Writable param) throws IOException;


    private Writable makeParam() {
        Writable param;
        try {
            param = (Writable) paramClass.newInstance();
            if (param instanceof Configurable) {
                ((Configurable) param).setConf(conf);
            }
        } catch (InstantiationException e) {
            throw new RuntimeException(e.toString());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e.toString());
        }
        return param;
    }

}
