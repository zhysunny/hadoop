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

package org.apache.hadoop.conf;

import java.util.*;
import java.net.URL;
import java.io.*;
import javax.xml.parsers.*;

import org.apache.hadoop.util.FilePathConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;

import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * 提供对配置参数的访问。配置由资源指定。资源包含一组name/value对。
 * 每个资源都是由字符串或文件命名的。如果使用字符串命名，则将检查类路径以查找具有该名称的文件。如果是文件，则直接检查文件系统，而不需要引用类路径。
 * 配置资源有两种类型:default和final。首先加载默认值，最后加载最终值，从而覆盖默认值。
 * Hadoop的默认资源是字符串“hadoop-default.xml”。它的最终资源是字符串“hadoop-site.xml”。Hadoop上构建的其他工具可能指定额外的资源。
 * @author 章云
 * @date 2019/7/30 8:55
 */
public class Configuration {
    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

    /**
     * 默认资源:hadoop-default.xml
     */
    private ArrayList<Object> defaultResources = new ArrayList<Object>();
    /**
     * 最终资源:hadoop-site.xml
     */
    private ArrayList<Object> finalResources = new ArrayList<Object>();
    /**
     * 配置集合
     */
    private Properties properties;
    /**
     * 类加载器
     */
    private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    /**
     * 实例化新的对象
     */
    public Configuration() {
        defaultResources.add(FilePathConstant.HADOOP_DEFAULT_XML);
        finalResources.add(FilePathConstant.HADOOP_SITE_XML);
    }

    /**
     * 从另一个复制具有相同设置的新配置。
     * @param other
     */
    public Configuration(Configuration other) {
        this.defaultResources = (ArrayList<Object>) other.defaultResources.clone();
        this.finalResources = (ArrayList<Object>) other.finalResources.clone();
        if (other.properties != null) {
            this.properties = (Properties) other.properties.clone();
        }
    }

    /**
     * 添加默认资源。
     * @param name
     */
    public void addDefaultResource(String name) {
        addResource(defaultResources, name);
    }

    /**
     * 添加默认资源。
     * @param file
     */
    public void addDefaultResource(File file) {
        addResource(defaultResources, file);
    }

    /**
     * 添加最终资源。
     * @param name
     */
    public void addFinalResource(String name) {
        addResource(finalResources, name);
    }

    /**
     * 添加最终资源。
     * @param file
     */
    public void addFinalResource(File file) {
        addResource(finalResources, file);
    }

    /**
     * 添加资源并初始化配置集合
     * @param resources
     * @param resource
     */
    private synchronized void addResource(ArrayList<Object> resources, Object resource) {
        resources.add(resource);
        properties = null;
    }

    /**
     * 返回name属性的值，如果不存在此类属性，则返回null。
     * @param name
     * @return
     */
    public Object getObject(String name) {
        return getProps().get(name);
    }

    /**
     * 设置name属性的值。
     * @param name
     * @param value
     */
    public void setObject(String name, Object value) {
        getProps().put(name, value);
    }

    /**
     * 返回name属性的值。如果不存在此类属性，则返回defaultValue。
     * @param name
     * @param defaultValue
     * @return
     */
    public Object get(String name, Object defaultValue) {
        Object res = getObject(name);
        if (res != null) {
            return res;
        } else {
            return defaultValue;
        }
    }

    /**
     * 设置name属性的值。
     * @param name
     * @param value
     */
    public void set(String name, Object value) {
        getProps().setProperty(name, value.toString());
    }

    /**
     * 返回name属性的值，如果不存在此类属性，则返回null。
     * @param name
     * @return
     */
    public String get(String name) {
        return getProps().getProperty(name);
    }

    /**
     * 返回name属性的值。如果不存在此类属性，则返回defaultValue。
     * @param name
     * @param defaultValue
     * @return
     */
    public String get(String name, String defaultValue) {
        return getProps().getProperty(name, defaultValue);
    }

    /**
     * 以整数形式返回name属性的值。如果没有指定此类属性，或者指定的值不是有效整数，则返回defaultValue。
     * @param name
     * @param defaultValue
     * @return
     */
    public int getInt(String name, int defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(valueString);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 将name属性的值设置为整数。
     * @param name
     * @param value
     */
    public void setInt(String name, int value) {
        set(name, Integer.toString(value));
    }


    /**
     * 以长整数形式返回name属性的值。如果没有指定此类属性，或者指定的值不是有效长整数，则返回defaultValue。
     * @param name
     * @param defaultValue
     * @return
     */
    public long getLong(String name, long defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(valueString);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 将name属性的值设置为长整数。
     * @param name
     * @param value
     */
    public void setLong(String name, long value) {
        set(name, Long.toString(value));
    }

    /**
     * 以单精度浮点型形式返回name属性的值。如果没有指定此类属性，或者指定的值不是有效单精度浮点型，则返回defaultValue。
     * @param name
     * @param defaultValue
     * @return
     */
    public float getFloat(String name, float defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(valueString);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 以布尔型形式返回name属性的值。如果没有指定此类属性，或者指定的值不是有效布尔型，则返回defaultValue。
     * @param name
     * @param defaultValue
     * @return
     */
    public boolean getBoolean(String name, boolean defaultValue) {
        String valueString = get(name);
        if ("true".equalsIgnoreCase(valueString)) {
            return true;
        } else if ("false".equalsIgnoreCase(valueString)) {
            return false;
        } else {
            return defaultValue;
        }
    }

    /**
     * 将name属性的值设置为布尔型。
     * @param name
     * @param value
     */
    public void setBoolean(String name, boolean value) {
        set(name, Boolean.toString(value));
    }

    /**
     * 以字符串数组的形式返回name属性的值。如果没有指定此类属性，则返回null。值由空格或逗号分隔。
     * @param name
     * @return
     */
    public String[] getStrings(String name) {
        String valueString = get(name);
        if (valueString == null) {
            return null;
        }
        StringTokenizer tokenizer = new StringTokenizer(valueString, ", \t\n\r\f");
        List<String> values = new ArrayList<String>();
        while (tokenizer.hasMoreTokens()) {
            values.add(tokenizer.nextToken());
        }
        return values.toArray(new String[values.size()]);
    }

    /**
     * 作为类返回name属性的值。如果没有指定此类属性，则返回defaultValue。
     * @param name
     * @param defaultValue
     * @return
     */
    public Class getClass(String name, Class defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }
        try {
            return Class.forName(valueString);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 作为类返回name属性的值。如果没有指定此类属性，则返回defaultValue。如果返回的类没有实现指定的接口，则会引发错误。
     * @param name         属性名
     * @param defaultValue 配置不存在时默认的类
     * @param xface        配置类的父类
     * @return
     */
    public Class getClass(String name, Class defaultValue, Class xface) {
        try {
            Class theClass = getClass(name, defaultValue);
            // 如果xface不是theClass的超类，抛出异常
            if (theClass != null && !xface.isAssignableFrom(theClass)) {
                throw new RuntimeException(theClass + " not " + xface.getName());
            }
            return theClass;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将name属性的值设置为类的名称。首先检查类是否实现了命名接口。
     * @param name
     * @param theClass
     * @param xface
     */
    public void setClass(String name, Class theClass, Class xface) {
        if (!xface.isAssignableFrom(theClass)) {
            throw new RuntimeException(theClass + " not " + xface.getName());
        }
        set(name, theClass.getName());
    }

    /**
     * 返回name属性的值对应的数组，数组元素是目录
     * 如果name属性的值包含多个目录，则根据path's hash code选择一个目录。
     * 如果所选目录不存在，则尝试创建它。
     * @param name
     * @param path
     * @return
     * @throws IOException
     */
    public File getFile(String name, String path) throws IOException {
        String[] dirs = getStrings(name);
        int hashCode = path.hashCode();
        for (int i = 0; i < dirs.length; i++) {
            // i & Integer.MAX_VALUE = i
            // 相当于优先获得与path的hashCode一样值的索引
            int index = (hashCode + i & Integer.MAX_VALUE) % dirs.length;
            File file = new File(dirs[index], path).getAbsoluteFile();
            File dir = file.getParentFile();
            if (dir.exists() || dir.mkdirs()) {
                // 只返回path对应的文件或目录
                return file;
            }
        }
        throw new IOException("No valid local directories in property: " + name);
    }


    /**
     * 返回指定资源的URL。
     * @param name
     * @return
     */
    public URL getResource(String name) {
        return classLoader.getResource(name);
    }

    /**
     * 返回一个附加到配置资源的输入流，该输入流具有给定的name。
     * @param name
     * @return
     */
    public InputStream getConfResourceAsInputStream(String name) {
        try {
            URL url = getResource(name);
            if (url == null) {
                LOGGER.info(name + " not found");
                return null;
            } else {
                LOGGER.info("found resource " + name + " at " + url);
            }
            return url.openStream();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 返回一个附加到配置资源的阅读器，该阅读器具有给定的name。
     * @param name
     * @return
     */
    public Reader getConfResourceAsReader(String name) {
        try {
            URL url = getResource(name);
            if (url == null) {
                LOGGER.info(name + " not found");
                return null;
            } else {
                LOGGER.info("found resource " + name + " at " + url);
            }
            return new InputStreamReader(url.openStream());
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 获得配置集合对象
     * @return
     */
    private synchronized Properties getProps() {
        if (properties == null) {
            Properties newProps = new Properties();
            loadResources(newProps, defaultResources, false, false);
            loadResources(newProps, finalResources, true, true);
            properties = newProps;
        }
        return properties;
    }

    /**
     * 加载xml配置到Properties
     * @param props
     * @param resources
     * @param reverse   xml集合是否反序加载
     * @param quiet     是否沉默，true的话加载不到xml不会抛异常
     */
    private void loadResources(Properties props, ArrayList<Object> resources, boolean reverse, boolean quiet) {
        ListIterator i = resources.listIterator(reverse ? resources.size() : 0);
        while (reverse ? i.hasPrevious() : i.hasNext()) {
            loadResource(props, reverse ? i.previous() : i.next(), quiet);
        }
    }

    /**
     * 加载xml配置到Properties
     * @param properties
     * @param name
     * @param quiet      是否沉默，true的话加载不到xml不会抛异常
     */
    private void loadResource(Properties properties, Object name, boolean quiet) {
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = null;
            if (name instanceof String) {
                // a CLASSPATH resource
                URL url = getResource((String) name);
                if (url != null) {
                    LOGGER.info("parsing " + url);
                    doc = builder.parse(url.toString());
                }
            } else if (name instanceof File) {
                // a file resource
                File file = (File) name;
                if (file.exists()) {
                    LOGGER.info("parsing " + file);
                    doc = builder.parse(file);
                }
            }
            if (doc == null) {
                if (quiet) {
                    return;
                }
                throw new RuntimeException(name + " not found");
            }

            Element root = doc.getDocumentElement();
            if (!"configuration".equals(root.getTagName())) {
                LOGGER.warn("bad conf file: top-level element not <configuration>");
            }
            NodeList props = root.getChildNodes();
            for (int i = 0; i < props.getLength(); i++) {
                Node propNode = props.item(i);
                if (!(propNode instanceof Element)) {
                    continue;
                }
                Element prop = (Element) propNode;
                if (!"property".equals(prop.getTagName())) {
                    LOGGER.warn("bad conf file: element not <property>");
                }
                NodeList fields = prop.getChildNodes();
                String attr = null;
                String value = null;
                for (int j = 0; j < fields.getLength(); j++) {
                    Node fieldNode = fields.item(j);
                    if (!(fieldNode instanceof Element)) {
                        continue;
                    }
                    Element field = (Element) fieldNode;
                    if ("name".equals(field.getTagName())) {
                        attr = ((Text) field.getFirstChild()).getData();
                    }
                    if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
                        value = ((Text) field.getFirstChild()).getData();
                    }
                }
                if (attr != null && value != null) {
                    properties.setProperty(attr, value);
                }
            }

        } catch (Exception e) {
            LOGGER.error("error parsing conf file: ", e);
            throw new RuntimeException(e);
        }

    }

    /**
     * 在此配置中写入非默认属性。
     * @param out
     * @throws IOException
     */
    public void write(OutputStream out) throws IOException {
        Properties properties = getProps();
        try {
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            Element conf = doc.createElement("configuration");
            doc.appendChild(conf);
            conf.appendChild(doc.createTextNode("\n"));
            for (Enumeration e = properties.keys(); e.hasMoreElements(); ) {
                String name = (String) e.nextElement();
                Object object = properties.get(name);
                String value = null;
                if (object instanceof String) {
                    value = (String) object;
                } else {
                    continue;
                }
                Element propNode = doc.createElement("property");
                conf.appendChild(propNode);

                Element nameNode = doc.createElement("name");
                nameNode.appendChild(doc.createTextNode(name));
                propNode.appendChild(nameNode);

                Element valueNode = doc.createElement("value");
                valueNode.appendChild(doc.createTextNode(value));
                propNode.appendChild(valueNode);

                conf.appendChild(doc.createTextNode("\n"));
            }

            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(out);
            TransformerFactory transFactory = TransformerFactory.newInstance();
            Transformer transformer = transFactory.newTransformer();
            transformer.transform(source, result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Configuration: \n\t");
        sb.append("defaults: ");
        toString(defaultResources, sb);
        sb.append("\n\t");
        sb.append("final: ");
        toString(finalResources, sb);
        return sb.toString();
    }

    private void toString(ArrayList<Object> resources, StringBuffer sb) {
        ListIterator<Object> i = resources.listIterator();
        while (i.hasNext()) {
            if (i.nextIndex() != 0) {
                sb.append(" , ");
            }
            Object obj = i.next();
            if (obj instanceof File) {
                sb.append(obj);
            } else {
                sb.append((String) obj);
            }
        }
    }

    /**
     * 初始化当前类，并打印所有配置信息到控制台
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        new Configuration().write(System.out);
    }

}
