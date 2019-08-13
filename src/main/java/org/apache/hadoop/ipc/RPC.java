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

import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import java.net.InetSocketAddress;
import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个简单的RPC机制。
 * 一个<i>protocol</i>是一个Java接口。
 * 所有参数和返回类型必须是：
 * <li>基本数据类型或无返回</li>
 * <li>String字符串</li>
 * <li>{@link Writable}</li>
 * <li>上述类型的数组/li>
 * </ul>
 * 协议中的所有方法都应该只抛出IOException。
 * 没有传输协议实例的字段数据。
 * @author 章云
 * @date 2019/8/13 9:01
 */
public class RPC {
    private static final Logger LOGGER = LoggerFactory.getLogger(RPC.class);

    private RPC() {
    }


    /**
     * 方法调用，包括方法名称及其参数。
     */
    private static class Invocation implements Writable, Configurable {
        private String methodName;
        private Class[] parameterClasses;
        private Object[] parameters;
        private Configuration conf;

        public Invocation() {
        }

        public Invocation(Method method, Object[] parameters) {
            this.methodName = method.getName();
            this.parameterClasses = method.getParameterTypes();
            this.parameters = parameters;
        }

        /**
         * 所调用方法的名称。
         */
        public String getMethodName() {
            return methodName;
        }

        /**
         * 参数类。
         */
        public Class[] getParameterClasses() {
            return parameterClasses;
        }

        /**
         * 参数实例。
         */
        public Object[] getParameters() {
            return parameters;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            methodName = UTF8.readString(in);
            parameters = new Object[in.readInt()];
            parameterClasses = new Class[parameters.length];
            ObjectWritable objectWritable = new ObjectWritable();
            for (int i = 0; i < parameters.length; i++) {
                parameters[i] = ObjectWritable.readObject(in, objectWritable, this.conf);
                parameterClasses[i] = objectWritable.getDeclaredClass();
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            UTF8.writeString(out, methodName);
            out.writeInt(parameterClasses.length);
            for (int i = 0; i < parameterClasses.length; i++) {
                ObjectWritable.writeObject(out, parameters[i], parameterClasses[i]);
            }
        }

        @Override
        public String toString() {
            StringBuffer buffer = new StringBuffer();
            buffer.append(methodName);
            buffer.append("(");
            for (int i = 0; i < parameters.length; i++) {
                if (i != 0) {
                    buffer.append(", ");
                }
                buffer.append(parameters[i]);
            }
            buffer.append(")");
            return buffer.toString();
        }

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public Configuration getConf() {
            return this.conf;
        }

    }

    private static Client CLIENT;

    private static class Invoker implements InvocationHandler {
        private InetSocketAddress address;

        public Invoker(InetSocketAddress address, Configuration conf) {
            this.address = address;
            CLIENT = (Client) conf.getObject(Client.class.getName());
            if (CLIENT == null) {
                CLIENT = new Client(ObjectWritable.class, conf);
                conf.setObject(Client.class.getName(), CLIENT);
            }
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            ObjectWritable value = (ObjectWritable) CLIENT.call(new Invocation(method, args), address);
            return value.get();
        }
    }

    /**
     * 构造实现指定协议的客户端代理对象，并在指定地址与服务器通信。
     */
    public static <T> T getProxy(Class<T> protocol, InetSocketAddress addr, Configuration conf) {
        return (T) Proxy.newProxyInstance(protocol.getClassLoader(), new Class[]{protocol}, new Invoker(addr, conf));
    }

    /**
     * 专家:对一组服务器进行多个并行调用。
     */
    public static Object[] call(Method method, Object[][] params, InetSocketAddress[] addrs, Configuration conf) throws IOException {
        Invocation[] invocations = new Invocation[params.length];
        for (int i = 0; i < params.length; i++) {
            invocations[i] = new Invocation(method, params[i]);
        }
        CLIENT = (Client) conf.getObject(Client.class.getName());
        if (CLIENT == null) {
            CLIENT = new Client(ObjectWritable.class, conf);
            conf.setObject(Client.class.getName(), CLIENT);
        }
        Writable[] wrappedValues = CLIENT.call(invocations, addrs);
        if (method.getReturnType() == Void.TYPE) {
            return null;
        }
        Object[] values = (Object[]) Array.newInstance(method.getReturnType(), wrappedValues.length);
        for (int i = 0; i < values.length; i++) {
            if (wrappedValues[i] != null) {
                values[i] = ((ObjectWritable) wrappedValues[i]).get();
            }
        }
        return values;
    }


    /**
     * 为监听端口的协议实现实例构造服务器。
     */
    public static Server getServer(final Object instance, final int port, Configuration conf) {
        return getServer(instance, port, 1, false, conf);
    }

    /**
     * 为监听端口的协议实现实例构造服务器。
     */
    public static Server getServer(final Object instance, final int port, final int numHandlers, final boolean verbose, Configuration conf) {
        return new Server(instance, conf, port, numHandlers, verbose);
    }

    /**
     * An RPC Server.
     */
    public static class Server extends org.apache.hadoop.ipc.Server {
        private Object instance;
        private Class implementation;
        private boolean verbose;

        /**
         * 构造RPC服务器。
         * @param instance 将调用其方法的实例
         * @param conf     要使用的配置
         * @param port     监听连接的端口
         */
        public Server(Object instance, Configuration conf, int port) {
            this(instance, conf, port, 1, false);
        }

        /**
         * 构造RPC服务器。
         * @param instance    将调用其方法的实例
         * @param conf        要使用的配置
         * @param port        监听连接的端口
         * @param numHandlers 要运行的方法处理程序线程的数目
         * @param verbose     是否应该记录每个调用
         */
        public Server(Object instance, Configuration conf, int port, int numHandlers, boolean verbose) {
            super(port, Invocation.class, numHandlers, conf);
            this.instance = instance;
            this.implementation = instance.getClass();
            this.verbose = verbose;
        }

        @Override
        public Writable call(Writable param) throws IOException {
            try {
                Invocation call = (Invocation) param;
                if (verbose) {
                    log("Call: " + call);
                }
                Method method = implementation.getMethod(call.getMethodName(), call.getParameterClasses());
                Object value = method.invoke(instance, call.getParameters());
                if (verbose) {
                    log("Return: " + value);
                }
                return new ObjectWritable(method.getReturnType(), value);
            } catch (InvocationTargetException e) {
                Throwable target = e.getTargetException();
                if (target instanceof IOException) {
                    throw (IOException) target;
                } else {
                    IOException ioe = new IOException(target.toString());
                    ioe.setStackTrace(target.getStackTrace());
                    throw ioe;
                }
            } catch (Throwable e) {
                IOException ioe = new IOException(e.toString());
                ioe.setStackTrace(e.getStackTrace());
                throw ioe;
            }
        }
    }

    private static void log(String value) {
        if (value != null && value.length() > 55) {
            value = value.substring(0, 55) + "...";
        }
        LOGGER.info(value);
    }

}
