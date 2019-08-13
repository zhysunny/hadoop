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

    private static final ThreadLocal SERVER = new ThreadLocal();

    /**
     * 返回在or null下调用的服务器实例。
     * 可以在{@link #call(Writable)}实现下调用，也可以在参数和返回值的{@link Writable}方法下调用。
     * 允许应用程序访问服务器上下文。
     */
    public static Server get() {
        return (Server) SERVER.get();
    }

    private int port;                               // port we listen on
    private int handlerCount;                       // number of handler threads
    private int maxQueuedCalls;                     // max number of queued calls
    private Class paramClass;                       // class of call parameters
    private Configuration conf;

    private int timeout;

    private boolean running = true;                 // true while server runs
    private LinkedList callQueue = new LinkedList(); // queued calls
    private Object callDequeued = new Object();     // used by wait/notify

    /**
     * 排队等待处理的调用。
     */
    private static class Call {
        private int id;                               // the client's call id
        private Writable param;                       // the parameter passed
        private Connection connection;                // connection to client

        public Call(int id, Writable param, Connection connection) {
            this.id = id;
            this.param = param;
            this.connection = connection;
        }
    }

    /**
     * 监听套接字，启动新的连接线程。
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
                    new Connection(socket.accept()).start(); // start a new connection
                } catch (SocketTimeoutException e) {      // ignore timeouts
                } catch (Exception e) {                   // log all other exceptions
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
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            this.setDaemon(true);
            this.setName("Server connection on port " + port + " from "
                    + socket.getInetAddress().getHostAddress());
        }

        @Override
        public void run() {
            LOGGER.info(getName() + ": starting");
            SERVER.set(Server.this);
            try {
                while (running) {
                    int id;
                    try {
                        id = in.readInt();                    // try to read an id
                    } catch (SocketTimeoutException e) {
                        continue;
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(getName() + " got #" + id);
                    }
                    Writable param = makeParam();           // read param
                    param.readFields(in);
                    Call call = new Call(id, param, this);
                    synchronized (callQueue) {
                        callQueue.addLast(call);              // queue the call
                        callQueue.notify();                   // wake up a waiting handler
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
     * 处理排队的调用。
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
                        call = (Call) callQueue.removeFirst(); // pop the queue
                    }

                    synchronized (callDequeued) {           // tell others we've dequeued
                        callDequeued.notify();
                    }

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(getName() + ": has #" + call.id + " from " +
                                call.connection.socket.getInetAddress().getHostAddress());
                    }

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
