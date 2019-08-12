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
         * Called by the connection thread when the call is complete and the
         * value or error string are available.  Notifies by default.
         */
        public synchronized void callComplete() {
            notify();                                 // notify caller
        }

        /**
         * Update lastActivity with the current time.
         */
        public synchronized void touch() {
            lastActivity = System.currentTimeMillis();
        }

        /**
         * Update lastActivity with the current time.
         */
        public synchronized void setResult(Writable value, String error) {
            this.value = value;
            this.error = error;
            this.done = true;
        }

    }

    /**
     * Thread that reads responses and notifies callers.  Each connection owns a
     * socket connected to a remote address.  Calls are multiplexed through this
     * socket: responses may be delivered out of order.
     */
    private class Connection extends Thread {
        private InetSocketAddress address;            // address of server
        private Socket socket;                        // connected socket
        private DataInputStream in;
        private DataOutputStream out;
        private Hashtable calls = new Hashtable();    // currently active calls
        private Call readingCall;
        private Call writingCall;

        public Connection(InetSocketAddress address) throws IOException {
            this.address = address;
            this.socket = new Socket(address.getAddress(), address.getPort());
            socket.setSoTimeout(timeout);
            this.in = new DataInputStream
                    (new BufferedInputStream
                            (new FilterInputStream(socket.getInputStream()) {
                                @Override
                                public int read(byte[] buf, int off, int len) throws IOException {
                                    int value = super.read(buf, off, len);
                                    if (readingCall != null) {
                                        readingCall.touch();
                                    }
                                    return value;
                                }
                            }));
            this.out = new DataOutputStream
                    (new BufferedOutputStream
                            (new FilterOutputStream(socket.getOutputStream()) {
                                @Override
                                public void write(byte[] buf, int o, int len) throws IOException {
                                    out.write(buf, o, len);
                                    if (writingCall != null) {
                                        writingCall.touch();
                                    }
                                }
                            }));
            this.setDaemon(true);
            this.setName("Client connection to "
                    + address.getAddress().getHostAddress()
                    + ":" + address.getPort());
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

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(getName() + " got value #" + id);
                    }

                    Call call = (Call) calls.remove(new Integer(id));
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
                // This is what happens when the remote side goes down
            } catch (Exception e) {
                LOGGER.error(getName() + " caught: " + e, e);
            } finally {
                close();
            }
        }

        /**
         * Initiates a call by sending the parameter to the remote server.
         * Note: this is not called from the Connection thread, but by other
         * threads.
         */
        public void sendParam(Call call) throws IOException {
            boolean error = true;
            try {
                calls.put(new Integer(call.id), call);
                synchronized (out) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(getName() + " sending #" + call.id);
                    }
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
                    close();                                // close on error
                }
            }
        }

        /**
         * Close the connection and remove it from the pool.
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
     * Call implementation used for parallel calls.
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
         * Deliver result to result collector.
         */
        @Override
        public void callComplete() {
            results.callComplete(this);
        }
    }

    /**
     * Result collector for parallel calls.
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
         * Collect a result.
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
     * Construct an IPC client whose values are of the given {@link Writable}
     * class.
     */
    public Client(Class<? extends Writable> valueClass, Configuration conf) {
        this.valueClass = valueClass;
        this.timeout = conf.getInt("ipc.client.timeout", 10000);
        this.conf = conf;
    }

    /**
     * Stop all threads related to this client.  No further calls may be made
     * using this client.
     */
    public void stop() {
        LOGGER.info("Stopping client");
        try {
            Thread.sleep(timeout);                        // let all calls complete
        } catch (InterruptedException e) {
        }
        running = false;
    }

    /**
     * Sets the timeout used for network i/o.
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * Make a call, passing <code>param</code>, to the IPC server running at
     * <code>address</code>, returning the value.  Throws exceptions if there are
     * network problems or if the remote code threw an exception.
     */
    public Writable call(Writable param, InetSocketAddress address)
            throws IOException {
        Connection connection = getConnection(address);
        Call call = new Call(param);
        synchronized (call) {
            connection.sendParam(call);                 // send the parameter
            long wait = timeout;
            do {
                try {
                    call.wait(wait);                       // wait for the result
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
     * Makes a set of calls in parallel.  Each parameter is sent to the
     * corresponding address.  When all values are available, or have timed out
     * or errored, the collected results are returned in an array.  The array
     * contains nulls for calls that timed out or errored.
     */
    public Writable[] call(Writable[] params, InetSocketAddress[] addresses)
            throws IOException {
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
                    LOGGER.info("Calling " + addresses[i] + " caught: " + e); // log errors
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
     * Get a connection from the pool, or create a new one and add it to the
     * pool.  Connections to a given host/port are reused.
     */
    private Connection getConnection(InetSocketAddress address)
            throws IOException {
        Connection connection;
        synchronized (connections) {
            connection = (Connection) connections.get(address);
            if (connection == null) {
                connection = new Connection(address);
                connections.put(address, connection);
                connection.start();
            }
        }
        return connection;
    }

    private Writable makeValue() {
        Writable value;                             // construct value
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
