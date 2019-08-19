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

import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

import java.io.IOException;
import java.io.EOFException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.rmi.RemoteException;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.UTF8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IPC服务的客户机。
 * IPC调用接受单个{@link Writable}作为参数，并返回一个{@link Writable}作为它们的值。
 * 服务在端口上运行，由参数类和值类定义。
 * @author 章云
 * @date 2019/8/12 11:40
 */
public class Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private Hashtable<InetSocketAddress, Connection> connections = new Hashtable<InetSocketAddress, Connection>();

    /**
     * 调用值的类
     */
    private Class<? extends Writable> valueClass;
    /**
     * 调用超时时间
     */
    private int timeout;
    /**
     * 调用id计数器
     */
    private int counter;
    private boolean running = true;
    private Configuration conf;

    /**
     * 等待值的调用。
     */
    private class Call {
        int id;                                       // call id
        Writable param;                               // parameter
        Writable value;                               // value, null if error
        String error;                                 // error, null if value
        long lastActivity;                            // time of last i/o
        boolean done;                                 // true when call is done

        protected Call(Writable param) {
            this.param = param;
            synchronized (Client.this) {
                this.id = counter++;
            }
            touch();
        }

        /**
         * 当调用完成且值或错误字符串可用时，由连接线程调用。
         * 默认情况下通知。
         */
        public synchronized void callComplete() {
            notify();
        }

        /**
         * 用当前时间更新lastActivity。
         */
        public synchronized void touch() {
            lastActivity = System.currentTimeMillis();
        }

        /**
         * 设置结果
         */
        public synchronized void setResult(Writable value, String error) {
            this.value = value;
            this.error = error;
            this.done = true;
        }

    }

    /**
     * 线程，读取响应并通知调用者。
     * 每个连接都拥有一个套接字，该套接字连接到一个远程地址。
     * 调用通过这个套接字进行多路复用:响应可能是无序传递的。
     */
    private class Connection extends Thread {
        private InetSocketAddress address;            // address of server
        private Socket socket;                        // connected socket
        private DataInputStream in;
        private DataOutputStream out;
        private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();    // currently active calls
        private Call readingCall;
        private Call writingCall;

        public Connection(InetSocketAddress address) throws IOException {
            this.address = address;
            this.socket = new Socket(address.getAddress(), address.getPort());
            socket.setSoTimeout(timeout);
            this.in = new DataInputStream(new BufferedInputStream(new FilterInputStream(socket.getInputStream()) {
                @Override
                public int read(byte[] buf, int off, int len) throws IOException {
                    int value = super.read(buf, off, len);
                    if (readingCall != null) {
                        readingCall.touch();
                    }
                    return value;
                }
            }));
            this.out = new DataOutputStream(new BufferedOutputStream(new FilterOutputStream(socket.getOutputStream()) {
                @Override
                public void write(byte[] buf, int o, int len) throws IOException {
                    out.write(buf, o, len);
                    if (writingCall != null) {
                        writingCall.touch();
                    }
                }
            }));
            this.setDaemon(true);
            this.setName("Client connection to " + address.getAddress().getHostAddress() + ":" + address.getPort());
        }

        @Override
        public void run() {
            LOGGER.info(getName() + ": starting");
            try {
                while (running) {
                    int id;
                    try {
                        id = in.readInt();                    // try to read an id
                    } catch (SocketTimeoutException e) {
                        continue;
                    }
                    LOGGER.info(getName() + " got value #" + id);
                    Call call = calls.remove(new Integer(id));
                    boolean isError = in.readBoolean();     // read if error
                    if (isError) {
                        UTF8 utf8 = new UTF8();
                        utf8.readFields(in);                  // read error string
                        call.setResult(null, utf8.toString());
                    } else {
                        Writable value = makeValue();
                        try {
                            readingCall = call;
                            if (value instanceof Configurable) {
                                ((Configurable) value).setConf(conf);
                            }
                            value.readFields(in);                 // read value
                        } finally {
                            readingCall = null;
                        }
                        call.setResult(value, null);
                    }
                    call.callComplete();                   // deliver result to caller
                }
            } catch (EOFException eof) {
                // 这是远端下降时的情况
            } catch (Exception e) {
                LOGGER.error(getName() + " caught: " + e, e);
            } finally {
                close();
            }
        }

        /**
         * 通过向远程服务器发送参数来启动调用。
         * 注意:这不是从连接线程调用的，而是由其他线程调用的。
         */
        public void sendParam(Call call) throws IOException {
            boolean error = true;
            try {
                calls.put(new Integer(call.id), call);
                synchronized (out) {
                    LOGGER.info(getName() + " sending #" + call.id);
                    try {
                        writingCall = call;
                        out.writeInt(call.id);
                        call.param.write(out);
                        out.flush();
                    } finally {
                        writingCall = null;
                    }
                }
                error = false;
            } finally {
                if (error) {
                    close();
                }
            }
        }

        /**
         * 关闭连接并将其从池中移除。
         */
        public void close() {
            LOGGER.info(getName() + ": closing");
            synchronized (connections) {
                connections.remove(address);              // remove connection
            }
            try {
                socket.close();                           // close socket
            } catch (IOException e) {
            }
        }

    }

    /**
     * 用于并行调用的调用实现。
     */
    private class ParallelCall extends Call {
        private ParallelResults results;
        private int index;

        public ParallelCall(Writable param, ParallelResults results, int index) {
            super(param);
            this.results = results;
            this.index = index;
        }

        /**
         * 将结果交付给结果收集器。
         */
        @Override
        public void callComplete() {
            results.callComplete(this);
        }
    }

    /**
     * 并行调用的结果收集器。
     */
    private static class ParallelResults {
        private Writable[] values;
        private int size;
        private int count;

        public ParallelResults(int size) {
            this.values = new Writable[size];
            this.size = size;
        }

        /**
         * 收集结果。
         */
        public synchronized void callComplete(ParallelCall call) {
            values[call.index] = call.value;            // store the value
            count++;                                    // count it
            if (count == size)                          // if all values are in
            {
                notify();                                 // then notify waiting caller
            }
        }
    }

    /**
     * 构造一个IPC客户机，其值属于给定的{@link Writable}类。
     */
    public Client(Class<? extends Writable> valueClass, Configuration conf) {
        this.valueClass = valueClass;
        this.timeout = conf.getInt("ipc.client.timeout", 10000);
        this.conf = conf;
    }

    /**
     * 停止与此客户机相关的所有线程。使用此客户端不能再进行任何调用。
     */
    public void stop() {
        LOGGER.info("Stopping client");
        try {
            // 让所有调用完成
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
        }
        running = false;
    }

    /**
     * 设置用于网络i/o的超时。
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * 调用，传递param，到运行在address的IPC服务器，返回值。
     * 如果存在网络问题或远程代码抛出异常，则抛出异常。
     */
    public Writable call(Writable param, InetSocketAddress address) throws IOException {
        Connection connection = getConnection(address);
        Call call = new Call(param);
        synchronized (call) {
            connection.sendParam(call);
            long wait = timeout;
            do {
                try {
                    call.wait(wait);
                } catch (InterruptedException e) {
                }
                wait = timeout - (System.currentTimeMillis() - call.lastActivity);
            } while (!call.done && wait > 0);

            if (call.error != null) {
                throw new RemoteException(call.error);
            } else if (!call.done) {
                throw new IOException("timed out waiting for response");
            } else {
                return call.value;
            }
        }
    }

    /**
     * 并行执行一组调用。
     * 每个参数都被发送到相应的地址。
     * 当所有值都可用时，或者超时或出错时，收集的结果将以数组的形式返回。
     * 数组包含超时或错误调用的空值。
     */
    public Writable[] call(Writable[] params, InetSocketAddress[] addresses) throws IOException {
        if (addresses.length == 0) {
            return new Writable[0];
        }
        ParallelResults results = new ParallelResults(params.length);
        synchronized (results) {
            for (int i = 0; i < params.length; i++) {
                ParallelCall call = new ParallelCall(params[i], results, i);
                try {
                    Connection connection = getConnection(addresses[i]);
                    connection.sendParam(call);             // send each parameter
                } catch (IOException e) {
                    LOGGER.error("Calling " + addresses[i] + " caught: " + e);
                    results.size--;                         //  wait for one fewer result
                }
            }
            try {
                results.wait(timeout);                    // wait for all results
            } catch (InterruptedException e) {
            }

            if (results.count == 0) {
                throw new IOException("no responses");
            } else {
                return results.values;
            }
        }
    }

    /**
     * 从池中获取连接，或者创建一个新的连接并将其添加到池中。
     * 到给定主机/端口的连接被重用。
     */
    private Connection getConnection(InetSocketAddress address) throws IOException {
        Connection connection;
        synchronized (connections) {
            connection = connections.get(address);
            if (connection == null) {
                connection = new Connection(address);
                connections.put(address, connection);
                connection.start();
            }
        }
        return connection;
    }

    private Writable makeValue() {
        Writable value;
        try {
            value = valueClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e.toString());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e.toString());
        }
        return value;
    }

}
