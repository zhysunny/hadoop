package org.apache.hadoop.conf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.constant.FilePathConstant;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 配置管理基类
 * @author 章云
 * @date 2019/6/20 21:40
 */
public class Configuration {
    private static final Log LOG = LogFactory.getLog(Configuration.class);
    private boolean quietmode = true;
    /**
     * 配置资源，支持URL，String(resources),Path,InputStream,Element
     */
    private ArrayList<Object> resources = new ArrayList<Object>();
    /**
     * 覆盖的属性name值
     */
    private Set<String> finalParameters = new HashSet<String>();
    /**
     * 默认加载
     */
    private boolean loadDefaults = true;
    /**
     * 默认资源配置文件名集合
     */
    private static final CopyOnWriteArrayList<String> defaultResources = new CopyOnWriteArrayList<String>();
    /**
     * updatingResource的value值
     */
    static final String UNKNOWN_RESOURCE = "Unknown";
    /**
     * 存储所有配置的name值，value值是加载这个配置的配置文件路径，如果自定义添加配置，value=UNKNOWN_RESOURCE
     */
    private HashMap<String, String> updatingResource;
    /**
     * 配置集合
     */
    private Properties properties;
    private ClassLoader classLoader;

    public synchronized Properties getProps() {
        if (properties == null) {
            properties = new Properties();
        }
        return properties;
    }

    /**
     * Get the {@link ClassLoader} for this job.
     * @return the correct class loader.
     */
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Set the class loader that will be used to load the various objects.
     * @param classLoader the new class loader.
     */
    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * 构造函数，默认loadDefaults=true
     */
    public Configuration() {
        this(true);
    }

    /**
     * 构造函数
     * @param loadDefaults
     */
    public Configuration(boolean loadDefaults) {
        this.loadDefaults = loadDefaults;
        updatingResource = new HashMap<String, String>();
        //默认加载的配置文件
        addDefaultResource(FilePathConstant.CORE_DEFAULT_XML);
        addDefaultResource(FilePathConstant.CORE_SITE_XML);
        if (loadDefaults) {
            // 加载配置
            for (String resource : defaultResources) {
                loadResource(getProps(), resource, quietmode);
            }
        }
        classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = Configuration.class.getClassLoader();
        }
    }

    /**
     * 构造函数，克隆other
     * @param other
     */
    @SuppressWarnings("unchecked")
    public Configuration(Configuration other) {
        this.resources = (ArrayList) other.resources.clone();
        synchronized (other) {
            if (other.properties != null) {
                this.properties = (Properties) other.properties.clone();
            }
            this.updatingResource = new HashMap<String, String>(other.updatingResource);
        }
        this.finalParameters = new HashSet<String>(other.finalParameters);
    }

    /**
     * 添加默认资源
     * @param name 文件名
     */
    public static synchronized void addDefaultResource(String name) {
        if (!defaultResources.contains(name)) {
            defaultResources.add(name);
        }
    }

    /**
     * 添加resources资源
     * @param name 相对文件路径
     */
    public void addResource(String name) {
        addResourceObject(name);
    }

    /**
     * 添加resources资源
     * @param url 资源url
     */
    @Deprecated
    public void addResource(URL url) {
        addResourceObject(url);
    }

    /**
     * 添加resources资源
     * @param file
     */
    public void addResource(Path file) {
        addResourceObject(file);
    }

    /**
     * 添加resources资源
     * @param in
     */
    @Deprecated
    public void addResource(InputStream in) {
        addResourceObject(in);
    }

    /**
     * 重新加载配置，清空配置项
     */
    public synchronized void reloadConfiguration() {
        properties = null;
        finalParameters.clear();
    }

    /**
     * 添加配置文件资源
     * @param resource
     */
    private synchronized void addResourceObject(Object resource) {
        // 加载配置
        loadResource(properties, resource, quietmode);
        resources.add(resource);
    }

    private static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
    private static int MAX_SUBST = 20;

    /**
     * 如果配置值中通过${}引用属性，这里将翻译这种引用，必须注意配置项顺序
     * @param expr 原始配置值
     * @return 返回翻译后的配置值
     */
    private String substituteVars(String expr) {
        if (expr == null) {
            return null;
        }
        Matcher match = varPat.matcher("");
        String eval = expr;
        for (int s = 0; s < MAX_SUBST; s++) {
            match.reset(eval);
            if (!match.find()) {
                return eval;
            }
            String var = match.group();
            var = var.substring(2, var.length() - 1);
            String val = null;
            try {
                val = System.getProperty(var);
            } catch (SecurityException se) {
                LOG.warn("Unexpected SecurityException in Configuration", se);
            }
            if (val == null) {
                val = getRaw(var);
            }
            if (val == null) {
                return eval;
            }
            eval = eval.substring(0, match.start()) + val + eval.substring(match.end());
        }
        throw new IllegalStateException("Variable substitution depth too large: "
                + MAX_SUBST + " " + expr);
    }

    /**
     * 通过属性名获得翻译后的配置值
     * @param name
     * @return
     */
    public String get(String name) {
        return substituteVars(getProps().getProperty(name));
    }

    /**
     * 通过属性名获得原始的配置值
     * @param name
     * @return
     */
    public String getRaw(String name) {
        return getProps().getProperty(name);
    }

    /**
     * 添加或覆盖配置属性
     * @param name  property name.
     * @param value property value.
     */
    public void set(String name, String value) {
        getProps().setProperty(name, value);
        this.updatingResource.put(name, UNKNOWN_RESOURCE);
    }

    /**
     * 删除配置项
     * @param name property name.
     */
    public synchronized void unset(String name) {
        getProps().remove(name);
    }

    /**
     * 当配置项不存在或为null时才增加配置
     * @param name  the property name
     * @param value the new value
     */
    public void setIfUnset(String name, String value) {
        if (get(name) == null) {
            set(name, value);
        }
    }

    /**
     * 获得配置值，如果值为null，返回defaultValue
     * @param name
     * @param defaultValue
     * @return
     */
    public String get(String name, String defaultValue) {
        return substituteVars(getProps().getProperty(name, defaultValue));
    }

    /**
     * 获得Int配置值，如果值为null，返回defaultValue
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
            String hexString = getHexDigits(valueString);
            if (hexString != null) {
                //16进制数转换
                return Integer.parseInt(hexString, 16);
            }
            //10进制数转换
            return Integer.parseInt(valueString);
        } catch (NumberFormatException e) {
            //如果配置值不是数字，返回defaultValue
            return defaultValue;
        }
    }

    /**
     * 添加数值型配置
     * @param name
     * @param value
     */
    public void setInt(String name, int value) {
        set(name, Integer.toString(value));
    }

    /**
     * 获得long配置值，如果值为null，返回defaultValue
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
            String hexString = getHexDigits(valueString);
            if (hexString != null) {
                return Long.parseLong(hexString, 16);
            }
            return Long.parseLong(valueString);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 如果是16进制数，返回去掉0x的部分，如果不是，返回null
     * @param value
     * @return
     */
    private String getHexDigits(String value) {
        boolean negative = false;
        String str = value;
        String hexString = null;
        if (value.startsWith("-")) {
            negative = true;
            str = value.substring(1);
        }
        if (str.startsWith("0x") || str.startsWith("0X")) {
            hexString = str.substring(2);
            if (negative) {
                hexString = "-" + hexString;
            }
            return hexString;
        }
        return null;
    }

    /**
     * 添加数值型配置
     * @param name
     * @param value
     */
    public void setLong(String name, long value) {
        set(name, Long.toString(value));
    }

    /**
     * 获得float配置值，如果值为null，返回defaultValue
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
     * 添加浮点型配置
     * @param name
     * @param value
     */
    public void setFloat(String name, float value) {
        set(name, Float.toString(value));
    }

    /**
     * 获得boolean配置值，如果值不是true或false，返回defaultValue
     * @param name
     * @param defaultValue
     * @return
     */
    public boolean getBoolean(String name, boolean defaultValue) {
        String valueString = get(name);
        if ("true".equals(valueString)) {
            return true;
        } else if ("false".equals(valueString)) {
            return false;
        } else {
            return defaultValue;
        }
    }

    /**
     * 添加布尔型配置
     * @param name
     * @param value
     */
    public void setBoolean(String name, boolean value) {
        set(name, Boolean.toString(value));
    }

    /**
     * 配置项不存在时才可以添加配置
     * @param name
     * @param value
     */
    public void setBooleanIfUnset(String name, boolean value) {
        setIfUnset(name, Boolean.toString(value));
    }

    /**
     * 配置值是枚举类
     * @param name
     * @param value
     * @param <T>
     */
    public <T extends Enum<T>> void setEnum(String name, T value) {
        set(name, value.toString());
    }

    /**
     * 配置值是枚举类
     * @param name
     * @param defaultValue
     * @param <T>
     * @return
     */
    public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
        final String val = get(name);
        return null == val
                ? defaultValue
                : Enum.valueOf(defaultValue.getDeclaringClass(), val);
    }

    /**
     * 静态内部类,获得一个数字范围列表
     */
    public static class IntegerRanges {
        private static class Range {
            int start;
            int end;
        }

        List<Range> ranges = new ArrayList<Range>();

        public IntegerRanges() {
        }

        public IntegerRanges(String newValue) {
            StringTokenizer itr = new StringTokenizer(newValue, ",");
            while (itr.hasMoreTokens()) {
                String rng = itr.nextToken().trim();
                String[] parts = rng.split("-", 3);
                // parts长度只能是1或2
                if (parts.length < 1 || parts.length > 2) {
                    throw new IllegalArgumentException("integer range badly formed: " +
                            rng);
                }
                Range r = new Range();
                r.start = convertToInt(parts[0], 0);
                if (parts.length == 2) {
                    r.end = convertToInt(parts[1], Integer.MAX_VALUE);
                } else {
                    // 如果parts长度为1，start=end
                    r.end = r.start;
                }
                if (r.start > r.end) {
                    throw new IllegalArgumentException("IntegerRange from " + r.start +
                            " to " + r.end + " is invalid");
                }
                ranges.add(r);
            }
        }

        /**
         * 把value转成int，如果value为空，返回defaultValue
         * @param value
         * @param defaultValue
         * @return
         */
        private static int convertToInt(String value, int defaultValue) {
            String trim = value.trim();
            if (trim.length() == 0) {
                return defaultValue;
            }
            return Integer.parseInt(trim);
        }

        /**
         * Is the given value in the set of ranges
         * @param value the value to check
         * @return is the value in the ranges?
         */
        public boolean isIncluded(int value) {
            for (Range r : ranges) {
                if (r.start <= value && value <= r.end) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuffer result = new StringBuffer();
            boolean first = true;
            for (Range r : ranges) {
                if (first) {
                    first = false;
                } else {
                    result.append(',');
                }
                result.append(r.start);
                result.append('-');
                result.append(r.end);
            }
            return result.toString();
        }
    }

    /**
     * Parse the given attribute as a set of integer ranges
     * @param name         the attribute name
     * @param defaultValue the default value if it is not set
     * @return a new set of ranges from the configured value
     */
    public IntegerRanges getRange(String name, String defaultValue) {
        return new IntegerRanges(get(name, defaultValue));
    }

    /**
     * 配置值按逗号分隔
     * @param name
     * @return
     */
    public Collection<String> getStringCollection(String name) {
        String valueString = get(name);
        return StringUtils.getStringCollection(valueString);
    }

    /**
     * 配置值按逗号分隔
     * @param name
     * @return
     */
    public String[] getStrings(String name) {
        String valueString = get(name);
        return StringUtils.getStrings(valueString);
    }

    /**
     * 配置值按逗号分隔，如果配置值为null，使用defaultValue
     * @param name
     * @param defaultValue
     * @return
     */
    public String[] getStrings(String name, String... defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        } else {
            return StringUtils.getStrings(valueString);
        }
    }

    /**
     * 设置配置值，用逗号分隔
     * @param name
     * @param values
     */
    public void setStrings(String name, String... values) {
        set(name, StringUtils.arrayToString(values));
    }

    /**
     * Load a class by name.
     * @param name the class name.
     * @return the class object.
     * @throws ClassNotFoundException if the class is not found.
     */
    public Class<?> getClassByName(String name) throws ClassNotFoundException {
        return Class.forName(name, true, classLoader);
    }

    /**
     * 把配置值的classname返回Class[]（多个classname）
     * @param name         the property name.
     * @param defaultValue default value.
     * @return property value as a <code>Class[]</code>,
     * or <code>defaultValue</code>.
     */
    public Class<?>[] getClasses(String name, Class<?>... defaultValue) {
        String[] classnames = getStrings(name);
        if (classnames == null) {
            return defaultValue;
        }
        try {
            Class<?>[] classes = new Class<?>[classnames.length];
            for (int i = 0; i < classnames.length; i++) {
                classes[i] = getClassByName(classnames[i]);
            }
            return classes;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 把配置值的classname返回Class（单个classname）
     * @param name         the class name.
     * @param defaultValue default value.
     * @return property value as a <code>Class</code>,
     * or <code>defaultValue</code>.
     */
    public Class<?> getClass(String name, Class<?> defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }
        try {
            return getClassByName(valueString);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param name         the class name.
     * @param defaultValue default value.
     * @param xface        the interface implemented by the named class.
     * @return property value as a <code>Class</code>,
     * or <code>defaultValue</code>.
     */
    public <U> Class<? extends U> getClass(String name,
                                           Class<? extends U> defaultValue,
                                           Class<U> xface) {
        try {
            Class<?> theClass = getClass(name, defaultValue);
            if (theClass != null && !xface.isAssignableFrom(theClass)) {
                // 如果theClass不为null，且theClass不是xface的子类
                throw new RuntimeException(theClass + " not " + xface.getName());
            } else if (theClass != null) {
                // 如果theClass不为null，且theClass是xface的子类
                // asSubclass方法测试theClass是否为xface的子类，不是抛ClassCastException异常
                return theClass.asSubclass(xface);
            } else {
                // 如果theClass为null
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the value of the <code>name</code> property as a <code>List</code>
     * of objects implementing the interface specified by <code>xface</code>.
     * <p>
     * An exception is thrown if any of the classes does not exist, or if it does
     * not implement the named interface.
     * @param name  the property name.
     * @param xface the interface implemented by the classes named by
     *              <code>name</code>.
     * @return a <code>List</code> of objects implementing <code>xface</code>.
     */
    @SuppressWarnings("unchecked")
    public <U> List<U> getInstances(String name, Class<U> xface) {
        List<U> ret = new ArrayList<U>();
        //获得配置中的classnames并返回Class[]
        Class<?>[] classes = getClasses(name);
        for (Class<?> cl : classes) {
            //循环数组，如果有Class不是xface的子类，抛出异常
            if (!xface.isAssignableFrom(cl)) {
                throw new RuntimeException(cl + " does not implement " + xface);
            }
            // 如果Class是xface的子类,则实例化
            ret.add((U) ReflectionUtils.newInstance(cl, this));
        }
        return ret;
    }

    /**
     * 添加一个配置
     * @param name     property name.
     * @param theClass property value.
     * @param xface    the interface implemented by the named class.
     */
    public void setClass(String name, Class<?> theClass, Class<?> xface) {
        if (!xface.isAssignableFrom(theClass)) {
            throw new RuntimeException(theClass + " not " + xface.getName());
        }
        set(name, theClass.getName());
    }

    /**
     * 返回本地文件jobid.xml对应的path，如果tmp/mapred/local/目录不存在会创建
     * @param dirsProp 目前dirsProp=mapred.local.dir即tmp/mapred/local/
     * @param path     jobid.xml
     * @return 返回本地文件jobid.xml对应的path
     */
    public Path getLocalPath(String dirsProp, String path) throws IOException {
        String[] dirs = getStrings(dirsProp);
        int hashCode = path.hashCode();
        FileSystem fs = FileSystem.getLocal(this);
        for (int i = 0; i < dirs.length; i++) {
            //一个数&Integer.MAX_VALUE不会变化
            //一般dirs.length==1，这里index还是为0
            //通过hash取模的方式，随机将path放到dirs的某个目录下
            int index = (hashCode + i & Integer.MAX_VALUE) % dirs.length;
            Path file = new Path(dirs[index], path);
            Path dir = file.getParent();
            if (fs.mkdirs(dir) || fs.exists(dir)) {
                return file;
            }
        }
        LOG.warn("Could not make " + path +
                " in local directories from " + dirsProp);
        for (int i = 0; i < dirs.length; i++) {
            int index = (hashCode + i & Integer.MAX_VALUE) % dirs.length;
            LOG.warn(dirsProp + "[" + index + "]=" + dirs[index]);
        }
        throw new IOException("No valid local directories in property: " + dirsProp);
    }

    /**
     * 与getLocalPath方法一样，返回File类型
     * @param dirsProp directory in which to locate the file.
     * @param path     file-path.
     * @return local file under the directory with the given path.
     */
    public File getFile(String dirsProp, String path) throws IOException {
        String[] dirs = getStrings(dirsProp);
        int hashCode = path.hashCode();
        for (int i = 0; i < dirs.length; i++) {
            int index = (hashCode + i & Integer.MAX_VALUE) % dirs.length;
            File file = new File(dirs[index], path);
            File dir = file.getParentFile();
            if (dir.exists() || dir.mkdirs()) {
                return file;
            }
        }
        throw new IOException("No valid local directories in property: " + dirsProp);
    }

    /**
     * Get the {@link URL} for the named resource.
     * @param name resource name.
     * @return the url for the named resource.
     */
    public URL getResource(String name) {
        return classLoader.getResource(name);
    }

    /**
     * Get an input stream attached to the configuration resource with the
     * given <code>name</code>.
     * @param name configuration resource name.
     * @return an input stream attached to the resource.
     */
    public InputStream getConfResourceAsInputStream(String name) {
        try {
            URL url = getResource(name);
            if (url == null) {
                LOG.info(name + " not found");
                return null;
            } else {
                LOG.info("found resource " + name + " at " + url);
            }
            return url.openStream();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get a {@link Reader} attached to the configuration resource with the
     * given <code>name</code>.
     * @param name configuration resource name.
     * @return a reader attached to the resource.
     */
    public Reader getConfResourceAsReader(String name) {
        try {
            URL url = getResource(name);
            if (url == null) {
                LOG.info(name + " not found");
                return null;
            } else {
                LOG.info("found resource " + name + " at " + url);
            }
            return new InputStreamReader(url.openStream());
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Return the number of keys in the configuration.
     * @return number of keys in the configuration.
     */
    public int size() {
        return getProps().size();
    }

    /**
     * Clears all keys from the configuration.
     */
    public void clear() {
        getProps().clear();
    }

    /**
     * 生成properties的迭代器
     * @return
     */
    public Iterator<Map.Entry<String, String>> iterator() {
        Map<String, String> result = new HashMap<String, String>();
        for (Map.Entry<Object, Object> item : getProps().entrySet()) {
            if (item.getKey() instanceof String &&
                    item.getValue() instanceof String) {
                result.put((String) item.getKey(), (String) item.getValue());
            }
        }
        return result.entrySet().iterator();
    }

    /**
     * 加载xml配置
     * @param properties key-value存储配置
     * @param name       资源，支持URL，String(resources),Path,InputStream,Element
     * @param quiet
     */
    private void loadResource(Properties properties, Object name, boolean quiet) {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            docBuilderFactory.setIgnoringComments(true);
            docBuilderFactory.setNamespaceAware(true);
            try {
                docBuilderFactory.setXIncludeAware(true);
            } catch (UnsupportedOperationException e) {
                LOG.error("Failed to set setXIncludeAware(true) for parser "
                                + docBuilderFactory
                                + ":" + e,
                        e);
            }
            DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
            Document doc = null;
            Element root = null;
            if (name instanceof URL) {
                URL url = (URL) name;
                if (url != null) {
                    if (!quiet) {
                        LOG.info("parsing " + url);
                    }
                    doc = builder.parse(url.toString());
                }
            } else if (name instanceof String) {
                File file = new File((String) name);
                LOG.debug("加载配置文件：" + file.getAbsolutePath());
                if (file.exists()) {
                    if (!quiet) {
                        LOG.info("parsing " + file);
                    }
                    doc = builder.parse(file);
                }
            } else if (name instanceof Path) {
                File file = new File(((Path) name).toUri().getPath())
                        .getAbsoluteFile();
                if (file.exists()) {
                    if (!quiet) {
                        LOG.info("parsing " + file);
                    }
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    try {
                        doc = builder.parse(in);
                    } finally {
                        in.close();
                    }
                }
            } else if (name instanceof InputStream) {
                try {
                    doc = builder.parse((InputStream) name);
                } finally {
                    ((InputStream) name).close();
                }
            } else if (name instanceof Element) {
                root = (Element) name;
            }
            if (doc == null && root == null) {
                if (quiet) {
                    return;
                }
                throw new RuntimeException(name + " not found");
            }
            if (root == null) {
                root = doc.getDocumentElement();
            }
            if (!"configuration".equals(root.getTagName())) {
                LOG.fatal("bad conf file: top-level element not <configuration>");
            }
            NodeList props = root.getChildNodes();
            for (int i = 0; i < props.getLength(); i++) {
                Node propNode = props.item(i);
                if (!(propNode instanceof Element)) {
                    continue;
                }
                Element prop = (Element) propNode;
                if ("configuration".equals(prop.getTagName())) {
                    loadResource(properties, prop, quiet);
                    continue;
                }
                if (!"property".equals(prop.getTagName())) {
                    LOG.warn("bad conf file: element not <property>");
                }
                NodeList fields = prop.getChildNodes();
                String attr = null;
                String value = null;
                boolean finalParameter = false;
                for (int j = 0; j < fields.getLength(); j++) {
                    Node fieldNode = fields.item(j);
                    if (!(fieldNode instanceof Element)) {
                        continue;
                    }
                    Element field = (Element) fieldNode;
                    if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
                        attr = ((Text) field.getFirstChild()).getData().trim();
                    }
                    if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
                        value = ((Text) field.getFirstChild()).getData();
                    }
                    if ("final".equals(field.getTagName()) && field.hasChildNodes()) {
                        finalParameter = "true".equals(((Text) field.getFirstChild()).getData());
                    }
                }
                if (attr != null) {
                    if (value != null) {
                        if (!finalParameters.contains(attr)) {
                            properties.setProperty(attr, value);
                            updatingResource.put(attr, name.toString());
                        } else if (!value.equals(properties.getProperty(attr))) {
                            LOG.warn(name + ":a attempt to override final parameter: " + attr
                                    + ";  Ignoring.");
                        }
                    }
                    if (finalParameter) {
                        finalParameters.add(attr);
                    }
                }
            }
        } catch (IOException e) {
            LOG.fatal("error parsing conf file: " + e);
            throw new RuntimeException(e);
        } catch (DOMException e) {
            LOG.fatal("error parsing conf file: " + e);
            throw new RuntimeException(e);
        } catch (SAXException e) {
            LOG.fatal("error parsing conf file: " + e);
            throw new RuntimeException(e);
        } catch (ParserConfigurationException e) {
            LOG.fatal("error parsing conf file: " + e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 将properties写入xml文件
     * @param out
     * @throws IOException
     */
    public void writeXml(OutputStream out) throws IOException {
        writeXml(new OutputStreamWriter(out));
    }

    /**
     * 将properties写入xml文件
     * @param out
     * @throws IOException
     */
    public void writeXml(Writer out) throws IOException {
        Document doc = asXmlDocument();
        try {
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(out);
            TransformerFactory transFactory = TransformerFactory.newInstance();
            Transformer transformer = transFactory.newTransformer();
            transformer.transform(source, result);
        } catch (TransformerException te) {
            throw new IOException(te);
        }
    }

    /**
     * 将properties配置写入xml
     * @return
     * @throws IOException
     */
    private synchronized Document asXmlDocument() throws IOException {
        Document doc;
        Properties properties = getProps();
        try {
            doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        } catch (ParserConfigurationException pe) {
            throw new IOException(pe);
        }
        Element conf = doc.createElement("configuration");
        doc.appendChild(conf);
        conf.appendChild(doc.createTextNode("\n"));
        for (Enumeration<Object> e = properties.keys(); e.hasMoreElements(); ) {
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
            if (updatingResource != null) {
                //注释，配置项原加载的哪个配置文件
                Comment commentNode = doc.createComment("Loaded from " + updatingResource.get(name));
                propNode.appendChild(commentNode);
            }
            Element nameNode = doc.createElement("name");
            nameNode.appendChild(doc.createTextNode(name));
            propNode.appendChild(nameNode);
            Element valueNode = doc.createElement("value");
            valueNode.appendChild(doc.createTextNode(value));
            propNode.appendChild(valueNode);
            conf.appendChild(doc.createTextNode("\n"));
        }
        return doc;
    }

    /**
     * 将properties配置写入json
     * @param out the Writer to write to
     * @throws IOException
     */
    public static void dumpConfiguration(Configuration config, Writer out) throws IOException {
        JsonFactory dumpFactory = new JsonFactory();
        JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
        dumpGenerator.writeStartObject();
        dumpGenerator.writeFieldName("properties");
        dumpGenerator.writeStartArray();
        dumpGenerator.flush();
        synchronized (config) {
            for (Map.Entry<Object, Object> item : config.getProps().entrySet()) {
                dumpGenerator.writeStartObject();
                dumpGenerator.writeStringField("key", (String) item.getKey());
                dumpGenerator.writeStringField("value", config.get((String) item.getKey()));
                dumpGenerator.writeBooleanField("isFinal", config.finalParameters.contains(item.getKey()));
                dumpGenerator.writeStringField("resource", config.updatingResource.get(item.getKey()));
                dumpGenerator.writeEndObject();
            }
        }
        dumpGenerator.writeEndArray();
        dumpGenerator.writeEndObject();
        dumpGenerator.flush();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Configuration: ");
        if (loadDefaults) {
            toString(defaultResources, sb);
            if (resources.size() > 0) {
                sb.append(", ");
            }
        }
        toString(resources, sb);
        return sb.toString();
    }

    private void toString(List resources, StringBuffer sb) {
        ListIterator i = resources.listIterator();
        while (i.hasNext()) {
            if (i.nextIndex() != 0) {
                sb.append(", ");
            }
            sb.append(i.next());
        }
    }

    /**
     * Set the quietness-mode.
     * <p>
     * In the quiet-mode, error and informational messages might not be logged.
     * @param quietmode <code>true</code> to set quiet-mode on, <code>false</code>
     *                  to turn it off.
     */
    public synchronized void setQuietMode(boolean quietmode) {
        this.quietmode = quietmode;
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        clear();
        int size = WritableUtils.readVInt(in);
        for (int i = 0; i < size; ++i) {
            set(org.apache.hadoop.io.Text.readString(in), org.apache.hadoop.io.Text.readString(in));
        }
    }

    @Deprecated
    public void write(DataOutput out) throws IOException {
        Properties props = getProps();
        WritableUtils.writeVInt(out, props.size());
        for (Map.Entry<Object, Object> item : props.entrySet()) {
            org.apache.hadoop.io.Text.writeString(out, (String) item.getKey());
            org.apache.hadoop.io.Text.writeString(out, (String) item.getValue());
        }
    }

    /**
     * 将properties配置中，key符合正则regex的配置返回
     * @param regex
     * @return
     */
    public Map<String, String> getValByRegex(String regex) {
        Pattern p = Pattern.compile(regex);
        Map<String, String> result = new HashMap<String, String>();
        Matcher m;
        for (Map.Entry<Object, Object> item : getProps().entrySet()) {
            if (item.getKey() instanceof String && item.getValue() instanceof String) {
                m = p.matcher((String) item.getKey());
                if (m.find()) {
                    result.put((String) item.getKey(), (String) item.getValue());
                }
            }
        }
        return result;
    }
}
