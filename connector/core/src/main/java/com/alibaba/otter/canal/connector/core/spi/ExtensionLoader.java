package com.alibaba.otter.canal.connector.core.spi;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SPI 类加载器
 *
 * @author rewerma 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class ExtensionLoader<T> {

    private static final Logger                                      logger                     = LoggerFactory.getLogger(ExtensionLoader.class);

    private static final String                                      SERVICES_DIRECTORY         = "META-INF/services/";

    private static final String                                      CANAL_DIRECTORY            = "META-INF/canal/";

    private static final String                                      DEFAULT_CLASSLOADER_POLICY = "internal";

    private static final Pattern                                     NAME_SEPARATOR             = Pattern.compile("\\s*[,]+\\s*");

    /**
     * 郁昊：扩展加载器缓存
     */
    private static final ConcurrentMap<Class<?>, ExtensionLoader<?>> EXTENSION_LOADERS          = new ConcurrentHashMap<>();

    /**
     * 郁昊：扩展类的实例 的缓存
     */
    private static final ConcurrentMap<Class<?>, Object>             EXTENSION_INSTANCES        = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, Object>               EXTENSION_KEY_INSTANCE     = new ConcurrentHashMap<>();

    /**
     * 郁昊：SPI接口类
     */
    private final Class<?>                                           type;

    private final String                                             classLoaderPolicy;

    private final ConcurrentMap<Class<?>, String>                    cachedNames                = new ConcurrentHashMap<>();

    private final Holder<Map<String, Class<?>>>                      cachedClasses              = new Holder<>();

    /**
     * Holder 与 服务模式(serverMode) 的映射缓存,Holder中保存着扩展类实例
     */
    private final ConcurrentMap<String, Holder<Object>>              cachedInstances            = new ConcurrentHashMap<>();

    /**
     * 郁昊：SPI类注解上的默认值 @SPI("kafka")
     */
    private String                                                   cachedDefaultName;

    private ConcurrentHashMap<String, IllegalStateException>         exceptions                 = new ConcurrentHashMap<>();

    /**
     * 郁昊：检查是否有SPI注解
     * @param type
     * @param <T>
     * @return
     */
    private static <T> boolean withExtensionAnnotation(Class<T> type) {
        return type.isAnnotationPresent(SPI.class);
    }

    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        return getExtensionLoader(type, DEFAULT_CLASSLOADER_POLICY);
    }

    /**
     * 郁昊：获取 扩展加载器
     * @param type SPI 接口类
     * @param classLoaderPolicy TODO
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type, String classLoaderPolicy) {
        if (type == null) throw new IllegalArgumentException("Extension type == null");
        /**
         * 传入的type需要是一个接口，去加载的是实现类
         */
        if (!type.isInterface()) {
            throw new IllegalArgumentException("Extension type(" + type + ") is not interface!");
        }
        /**
         * 如果SPI接口类没有拓展注解（SPI），则抛出异常
         */
        if (!withExtensionAnnotation(type)) {
            throw new IllegalArgumentException("Extension type(" + type + ") is not extension, because WITHOUT @"
                                               + SPI.class.getSimpleName() + " Annotation!");
        }

        /**
         * 郁昊：先尝试从缓存获取，有则返回，没有 new一个出来
         */
        ExtensionLoader<T> loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
        if (loader == null) {
            EXTENSION_LOADERS.putIfAbsent(type, new ExtensionLoader<T>(type, classLoaderPolicy));
            loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
        }
        return loader;
    }

    public ExtensionLoader(Class<?> type){
        this.type = type;
        this.classLoaderPolicy = DEFAULT_CLASSLOADER_POLICY;
    }

    public ExtensionLoader(Class<?> type, String classLoaderPolicy){
        this.type = type;
        this.classLoaderPolicy = classLoaderPolicy;
    }

    /**
     * 返回指定名字的扩展
     * @param name 服务模式
     * @param spiDir
     * @param standbyDir
     * @return
     */
    @SuppressWarnings("unchecked")
    public T getExtension(String name, String spiDir, String standbyDir) {
        if (name == null || name.length() == 0) throw new IllegalArgumentException("Extension name == null");
        /**
         * 郁昊：TODO 此次暂且不表
         */
        if ("true".equals(name)) {
            return getDefaultExtension(spiDir, standbyDir);
        }
        /**
         * 郁昊：先尝试从缓存获取 服务模式对应的Holder，如果没有new一个出来
         */
        Holder<Object> holder = cachedInstances.get(name);
        if (holder == null) {
            cachedInstances.putIfAbsent(name, new Holder<>());
            holder = cachedInstances.get(name);
        }
        /**
         * 双检锁，保证instance的线程安全，注意instance是volatile的值
         */
        Object instance = holder.get();
        if (instance == null) {
            synchronized (holder) {
                instance = holder.get();
                if (instance == null) {
                    instance = createExtension(name, spiDir, standbyDir);
                    holder.set(instance);
                }
            }
        }
        return (T) instance;
    }

    @SuppressWarnings("unchecked")
    public T getExtension(String name, String key, String spiDir, String standbyDir) {
        if (name == null || name.length() == 0) throw new IllegalArgumentException("Extension name == null");
        if ("true".equals(name)) {
            return getDefaultExtension(spiDir, standbyDir);
        }
        String extKey = name + "-" + StringUtils.trimToEmpty(key);
        Holder<Object> holder = cachedInstances.get(extKey);
        if (holder == null) {
            cachedInstances.putIfAbsent(extKey, new Holder<>());
            holder = cachedInstances.get(extKey);
        }
        Object instance = holder.get();
        if (instance == null) {
            synchronized (holder) {
                instance = holder.get();
                if (instance == null) {
                    instance = createExtension(name, key, spiDir, standbyDir);
                    holder.set(instance);
                }
            }
        }
        return (T) instance;
    }

    /**
     * 返回缺省的扩展，如果没有设置则返回<code>null</code>
     */
    public T getDefaultExtension(String spiDir, String standbyDir) {
        getExtensionClasses(spiDir, standbyDir);
        if (null == cachedDefaultName || cachedDefaultName.length() == 0 || "true".equals(cachedDefaultName)) {
            return null;
        }
        return getExtension(cachedDefaultName, spiDir, standbyDir);
    }

    /**
     *
     * @param name
     * @param spiDir
     * @param standbyDir
     * @return
     */
    @SuppressWarnings("unchecked")
    public T createExtension(String name, String spiDir, String standbyDir) {
        Class<?> clazz = getExtensionClasses(spiDir, standbyDir).get(name);
        if (clazz == null) {
            throw new IllegalStateException("Extension instance(name: " + name + ", class: " + type
                                            + ")  could not be instantiated: class could not be found");
        }
        try {
            T instance = (T) EXTENSION_INSTANCES.get(clazz);
            if (instance == null) {
                EXTENSION_INSTANCES.putIfAbsent(clazz, (T) clazz.newInstance());
                instance = (T) EXTENSION_INSTANCES.get(clazz);
            }
            return instance;
        } catch (Throwable t) {
            throw new IllegalStateException("Extension instance(name: " + name + ", class: " + type
                                            + ")  could not be instantiated: " + t.getMessage(), t);
        }
    }

    @SuppressWarnings("unchecked")
    private T createExtension(String name, String key, String spiDir, String standbyDir) {
        Class<?> clazz = getExtensionClasses(spiDir, standbyDir).get(name);
        if (clazz == null) {
            throw new IllegalStateException("Extension instance(name: " + name + ", class: " + type
                                            + ")  could not be instantiated: class could not be found");
        }
        try {
            T instance = (T) EXTENSION_KEY_INSTANCE.get(name + "-" + key);
            if (instance == null) {
                EXTENSION_KEY_INSTANCE.putIfAbsent(name + "-" + key, clazz.newInstance());
                instance = (T) EXTENSION_KEY_INSTANCE.get(name + "-" + key);
            }
            return instance;
        } catch (Throwable t) {
            throw new IllegalStateException("Extension instance(name: " + name + ", class: " + type
                                            + ")  could not be instantiated: " + t.getMessage(), t);
        }
    }

    private Map<String, Class<?>> getExtensionClasses(String spiDir, String standbyDir) {
        Map<String, Class<?>> classes = cachedClasses.get();
        if (classes == null) {
            synchronized (cachedClasses) {
                classes = cachedClasses.get();
                if (classes == null) {
                    classes = loadExtensionClasses(spiDir, standbyDir);
                    cachedClasses.set(classes);
                }
            }
        }

        return classes;
    }

    /**
     * 获得当前classpath的路径
     * @return
     */
    private String getJarDirectoryPath() {
        URL url = Thread.currentThread().getContextClassLoader().getResource("");
        String dirtyPath;
        if (url != null) {
            dirtyPath = url.toString();
        } else {
            File file = new File("");
            dirtyPath = file.getAbsolutePath();
        }
        String jarPath = dirtyPath.replaceAll("^.*file:/", ""); // removes
                                                                // file:/ and
                                                                // everything
                                                                // before it
        jarPath = jarPath.replaceAll("jar!.*", "jar"); // removes everything
                                                       // after .jar, if .jar
                                                       // exists in dirtyPath
        jarPath = jarPath.replaceAll("%20", " "); // necessary if path has
                                                  // spaces within
        if (!jarPath.endsWith(".jar")) { // this is needed if you plan to run
                                         // the app using Spring Tools Suit play
                                         // button.
            jarPath = jarPath.replaceAll("/classes/.*", "/classes/");
        }
        Path path = Paths.get(jarPath).getParent(); // Paths - from java 8
        if (path != null) {
            return path.toString();
        }
        return null;
    }

    /**
     *
     * @param spiDir
     * @param standbyDir
     * @return
     */
    private Map<String, Class<?>> loadExtensionClasses(String spiDir, String standbyDir) {
        /**
         * 郁昊：获取SPI接口类的注解，获取默认的 扩展名称 @SPI("kafka")
         */
        final SPI defaultAnnotation = type.getAnnotation(SPI.class);
        if (defaultAnnotation != null) {
            String value = defaultAnnotation.value();
            if ((value = value.trim()).length() > 0) {
                String[] names = NAME_SEPARATOR.split(value);
                if (names.length > 1) {
                    throw new IllegalStateException("more than 1 default extension name on extension " + type.getName()
                                                    + ": " + Arrays.toString(names));
                }
                if (names.length == 1) cachedDefaultName = names[0];
            }
        }

        Map<String, Class<?>> extensionClasses = new HashMap<>();

        /**
         * spiDir目录 和 standbyDir目录不为空的情况下
         */
        if (spiDir != null && standbyDir != null) {
            /**
             * getJarDirectoryPath为 当前classpath路径
             * 先尝试获取spiDir文件夹，如果不存在去获取standbyDir文件夹
             */
            // 1. plugin folder，customized extension classLoader
            // （jar_dir/plugin）
            String dir = File.separator + this.getJarDirectoryPath() + spiDir; // +
                                                                               // "plugin";

            File externalLibDir = new File(dir);
            if (!externalLibDir.exists()) {
                externalLibDir = new File(File.separator + this.getJarDirectoryPath() + standbyDir);
            }
            logger.info("extension classpath dir: " + externalLibDir.getAbsolutePath());
            if (externalLibDir.exists()) {
                /**
                 * 获取目录下面的所有jar
                 */
                File[] files = externalLibDir.listFiles((dir1, name) -> name.endsWith(".jar"));
                if (files != null) {
                    for (File f : files) {
                        URL url;
                        try {
                            url = f.toURI().toURL();
                        } catch (MalformedURLException e) {
                            throw new RuntimeException("load extension jar failed!", e);
                        }

                        /**
                         * 郁昊：获取当前线程的ClassLoader
                         */
                        ClassLoader parent = Thread.currentThread().getContextClassLoader();
                        URLClassLoader localClassLoader;
                        if (classLoaderPolicy == null || "".equals(classLoaderPolicy)
                            || DEFAULT_CLASSLOADER_POLICY.equalsIgnoreCase(classLoaderPolicy)) {
                            localClassLoader = new URLClassExtensionLoader(new URL[] { url });
                        } else {
                            localClassLoader = new URLClassLoader(new URL[] { url }, parent);
                        }

                        loadFile(extensionClasses, CANAL_DIRECTORY, localClassLoader);
                        loadFile(extensionClasses, SERVICES_DIRECTORY, localClassLoader);
                    }
                }
            }
        }

        // 2. load inner extension class with default classLoader
        ClassLoader classLoader = findClassLoader();
        loadFile(extensionClasses, CANAL_DIRECTORY, classLoader);
        loadFile(extensionClasses, SERVICES_DIRECTORY, classLoader);

        return extensionClasses;
    }

    private void loadFile(Map<String, Class<?>> extensionClasses, String dir, ClassLoader classLoader) {
        String fileName = dir + type.getName();
        try {
            Enumeration<URL> urls;
            if (classLoader != null) {
                urls = classLoader.getResources(fileName);
            } else {
                urls = ClassLoader.getSystemResources(fileName);
            }
            if (urls != null) {
                while (urls.hasMoreElements()) {
                    URL url = urls.nextElement();
                    try {
                        BufferedReader reader = null;
                        try {
                            reader = new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8));
                            String line = null;
                            while ((line = reader.readLine()) != null) {
                                final int ci = line.indexOf('#');
                                if (ci >= 0) line = line.substring(0, ci);
                                line = line.trim();
                                if (line.length() > 0) {
                                    try {
                                        String name = null;
                                        int i = line.indexOf('=');
                                        if (i > 0) {
                                            name = line.substring(0, i).trim();
                                            line = line.substring(i + 1).trim();
                                        }
                                        if (line.length() > 0) {
                                            Class<?> clazz = classLoader.loadClass(line);
                                            // Class<?> clazz =
                                            // Class.forName(line, true,
                                            // classLoader);
                                            if (!type.isAssignableFrom(clazz)) {
                                                throw new IllegalStateException("Error when load extension class(interface: "
                                                                                + type
                                                                                + ", class line: "
                                                                                + clazz.getName()
                                                                                + "), class "
                                                                                + clazz.getName()
                                                                                + "is not subtype of interface.");
                                            } else {
                                                try {
                                                    clazz.getConstructor(type);
                                                } catch (NoSuchMethodException e) {
                                                    clazz.getConstructor();
                                                    String[] names = NAME_SEPARATOR.split(name);
                                                    if (names != null && names.length > 0) {
                                                        for (String n : names) {
                                                            if (!cachedNames.containsKey(clazz)) {
                                                                cachedNames.put(clazz, n);
                                                            }
                                                            Class<?> c = extensionClasses.get(n);
                                                            if (c == null) {
                                                                extensionClasses.put(n, clazz);
                                                            } else if (c != clazz) {
                                                                cachedNames.remove(clazz);
                                                                throw new IllegalStateException("Duplicate extension "
                                                                                                + type.getName()
                                                                                                + " name " + n + " on "
                                                                                                + c.getName() + " and "
                                                                                                + clazz.getName());
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } catch (Throwable t) {
                                        IllegalStateException e = new IllegalStateException("Failed to load extension class(interface: "
                                                                                            + type
                                                                                            + ", class line: "
                                                                                            + line
                                                                                            + ") in "
                                                                                            + url
                                                                                            + ", cause: "
                                                                                            + t.getMessage(),
                                            t);
                                        exceptions.put(line, e);
                                    }
                                }
                            } // end of while read lines
                        } finally {
                            if (reader != null) {
                                reader.close();
                            }
                        }
                    } catch (Throwable t) {
                        logger.error("Exception when load extension class(interface: " + type + ", class file: " + url
                                     + ") in " + url, t);
                    }
                } // end of while urls
            }
        } catch (Throwable t) {
            logger.error("Exception when load extension class(interface: " + type + ", description file: " + fileName
                         + ").", t);
        }
    }

    private static ClassLoader findClassLoader() {
        return ExtensionLoader.class.getClassLoader();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" + type.getName() + "]";
    }

    private static class Holder<T> {

        private volatile T value;

        private void set(T value) {
            this.value = value;
        }

        private T get() {
            return value;
        }

    }
}
