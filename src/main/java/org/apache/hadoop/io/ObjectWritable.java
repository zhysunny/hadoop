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

import java.lang.reflect.Array;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;

/**
 * 一种多态可写程序，它用实例的类名来写实例。
 * 在没有可写包装器的情况下处理数组、字符串和基本类型。
 * @author 章云
 * @date 2019/8/30 9:43
 */
public class ObjectWritable implements Writable, Configurable {

    private Class declaredClass;
    private Object instance;
    private Configuration conf;

    public ObjectWritable() {
    }

    public ObjectWritable(Object instance) {
        set(instance);
    }

    public ObjectWritable(Class declaredClass, Object instance) {
        this.declaredClass = declaredClass;
        this.instance = instance;
    }

    public Object get() {
        return instance;
    }

    public Class getDeclaredClass() {
        return declaredClass;
    }

    public void set(Object instance) {
        this.declaredClass = instance.getClass();
        this.instance = instance;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readObject(in, this, this.conf);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeObject(out, instance, declaredClass);
    }

    private static final Map PRIMITIVE_NAMES = new HashMap();

    static {
        PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
        PRIMITIVE_NAMES.put("byte", Byte.TYPE);
        PRIMITIVE_NAMES.put("char", Character.TYPE);
        PRIMITIVE_NAMES.put("short", Short.TYPE);
        PRIMITIVE_NAMES.put("int", Integer.TYPE);
        PRIMITIVE_NAMES.put("long", Long.TYPE);
        PRIMITIVE_NAMES.put("float", Float.TYPE);
        PRIMITIVE_NAMES.put("double", Double.TYPE);
        PRIMITIVE_NAMES.put("void", Void.TYPE);
    }

    private static class NullInstance implements Writable {
        private Class declaredClass;

        public NullInstance() {
        }

        public NullInstance(Class declaredClass) {
            this.declaredClass = declaredClass;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            String className = UTF8.readString(in);
            declaredClass = (Class) PRIMITIVE_NAMES.get(className);
            if (declaredClass == null) {
                try {
                    declaredClass = Class.forName(className);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e.toString());
                }
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            UTF8.writeString(out, declaredClass.getName());
        }
    }

    /**
     * 编写一个{@link Writable}、{@link String}、基本类型或前面的数组。
     */
    public static void writeObject(DataOutput out, Object instance, Class declaredClass) throws IOException {
        if (instance == null) {
            instance = new NullInstance(declaredClass);
            declaredClass = NullInstance.class;
        }
        if (instance instanceof Writable) {
            // 编写instance的类，以支持声明类的子类
            UTF8.writeString(out, instance.getClass().getName());
            ((Writable) instance).write(out);
            return;
        }
        // 为原语编写声明的类，因为它们不能子类化，而且实例的类可能是包装器
        UTF8.writeString(out, declaredClass.getName());
        if (declaredClass.isArray()) {
            int length = Array.getLength(instance);
            out.writeInt(length);
            for (int i = 0; i < length; i++) {
                writeObject(out, Array.get(instance, i),
                        declaredClass.getComponentType());
            }
        } else if (declaredClass == String.class) {
            UTF8.writeString(out, (String) instance);
        } else if (declaredClass.isPrimitive()) {
            if (declaredClass == Boolean.TYPE) {
                out.writeBoolean(((Boolean) instance).booleanValue());
            } else if (declaredClass == Character.TYPE) {
                out.writeChar(((Character) instance).charValue());
            } else if (declaredClass == Byte.TYPE) {
                out.writeByte(((Byte) instance).byteValue());
            } else if (declaredClass == Short.TYPE) {
                out.writeShort(((Short) instance).shortValue());
            } else if (declaredClass == Integer.TYPE) {
                out.writeInt(((Integer) instance).intValue());
            } else if (declaredClass == Long.TYPE) {
                out.writeLong(((Long) instance).longValue());
            } else if (declaredClass == Float.TYPE) {
                out.writeFloat(((Float) instance).floatValue());
            } else if (declaredClass == Double.TYPE) {
                out.writeDouble(((Double) instance).doubleValue());
            } else if (declaredClass == Void.TYPE) {
            } else {
                throw new IllegalArgumentException("Not a primitive: " + declaredClass);
            }
        } else {
            throw new IOException("Can't write: " + instance + " as " + declaredClass);
        }
    }


    /**
     * 读取{@link Writable}、{@link String}、基本类型或前面的数组。
     */
    public static Object readObject(DataInput in, Configuration conf) throws IOException {
        return readObject(in, null, conf);
    }

    /**
     * 读取{@link Writable}、{@link String}、基本类型或前面的数组。
     */
    public static Object readObject(DataInput in, ObjectWritable objectWritable, Configuration conf) throws IOException {
        String className = UTF8.readString(in);
        Class declaredClass = (Class) PRIMITIVE_NAMES.get(className);
        if (declaredClass == null) {
            try {
                declaredClass = Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e.toString());
            }
        }
        Object instance;
        if (declaredClass == NullInstance.class) {
            NullInstance wrapper = new NullInstance();
            wrapper.readFields(in);
            declaredClass = wrapper.declaredClass;
            instance = null;

        } else if (declaredClass.isPrimitive()) {
            if (declaredClass == Boolean.TYPE) {
                instance = Boolean.valueOf(in.readBoolean());
            } else if (declaredClass == Character.TYPE) {
                instance = new Character(in.readChar());
            } else if (declaredClass == Byte.TYPE) {
                instance = new Byte(in.readByte());
            } else if (declaredClass == Short.TYPE) {
                instance = new Short(in.readShort());
            } else if (declaredClass == Integer.TYPE) {
                instance = new Integer(in.readInt());
            } else if (declaredClass == Long.TYPE) {
                instance = new Long(in.readLong());
            } else if (declaredClass == Float.TYPE) {
                instance = new Float(in.readFloat());
            } else if (declaredClass == Double.TYPE) {
                instance = new Double(in.readDouble());
            } else if (declaredClass == Void.TYPE) {
                instance = null;
            } else {
                throw new IllegalArgumentException("Not a primitive: " + declaredClass);
            }
        } else if (declaredClass.isArray()) {
            int length = in.readInt();
            instance = Array.newInstance(declaredClass.getComponentType(), length);
            for (int i = 0; i < length; i++) {
                Array.set(instance, i, readObject(in, conf));
            }
        } else if (declaredClass == String.class) {
            instance = UTF8.readString(in);
        } else {
            Writable writable = WritableFactories.newInstance(declaredClass);
            if (writable instanceof Configurable) {
                ((Configurable) writable).setConf(conf);
            }
            writable.readFields(in);
            instance = writable;
        }
        if (objectWritable != null) {
            objectWritable.declaredClass = declaredClass;
            objectWritable.instance = instance;
        }
        return instance;

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
