package com.alibaba.otter.canal.deployer;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanal;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanalConfigClient;

/**
 * canal独立版本启动的入口类
 *
 * @author jianghang 2012-11-6 下午05:20:49
 * @version 1.0.0
 */
public class CanalLauncher {

    private static final String             CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger             logger               = LoggerFactory.getLogger(CanalLauncher.class);
    public static final CountDownLatch      runningLatch         = new CountDownLatch(1);
    /**
     * 郁昊：定时执行线程池
     */
    private static ScheduledExecutorService executor             = Executors.newScheduledThreadPool(1,
                                                                     new NamedThreadFactory("canal-server-scan"));

    public static void main(String[] args) {
        try {
            /**
             * 郁昊：设置一个全局异常捕获handler
             * 先由线程来捕获异常
             * 再由线程组来捕获异常
             * 最终由全局默认handler捕获异常
             */
            logger.info("## set default uncaught exception handler");
            setGlobalUncaughtExceptionHandler();

            logger.info("## load canal configurations");
            /**
             * 郁昊：这里来加载配置文件
             * 如果通过 -D 的方式指定了配置文件（System.getProperty）则使用，否则使用默认文件
             */
            String conf = System.getProperty("canal.conf", "classpath:canal.properties");
            Properties properties = new Properties();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                /**
                 * 郁昊：基于classpath查找文件
                 */
                properties.load(CanalLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                /**
                 * 郁昊：TODO
                 */
                properties.load(new FileInputStream(conf));
            }

            /**
             * 郁昊：初始化 canalStater
             */
            final CanalStarter canalStater = new CanalStarter(properties);
            String managerAddress = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
            /**
             * canal管理端口 配置不为空的情况下
             */
            if (StringUtils.isNotEmpty(managerAddress)) {
                String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
                String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
                String adminPort = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT, "11110");
                boolean autoRegister = BooleanUtils.toBoolean(CanalController.getProperty(properties,
                    CanalConstants.CANAL_ADMIN_AUTO_REGISTER));
                String autoCluster = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_AUTO_CLUSTER);
                String name = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_REGISTER_NAME);
                String registerIp = CanalController.getProperty(properties, CanalConstants.CANAL_REGISTER_IP);
                if (StringUtils.isEmpty(registerIp)) {
                    registerIp = AddressUtils.getHostIp();
                }
                final PlainCanalConfigClient configClient = new PlainCanalConfigClient(managerAddress,
                    user,
                    passwd,
                    registerIp,
                    Integer.parseInt(adminPort),
                    autoRegister,
                    autoCluster,
                    name);
                /**
                 * 去CANAL_ADMIN_MANAGER请求canal配置
                 */
                PlainCanal canalConfig = configClient.findServer(null);
                if (canalConfig == null) {
                    throw new IllegalArgumentException("managerAddress:" + managerAddress
                                                       + " can't not found config for [" + registerIp + ":" + adminPort
                                                       + "]");
                }
                Properties managerProperties = canalConfig.getProperties();
                // merge local
                managerProperties.putAll(properties);
                /**
                 * 获取扫描的间隔，定期扫描最新配置文件，如果有新的配置则重新加载整个应用
                 */
                int scanIntervalInSecond = Integer.valueOf(CanalController.getProperty(managerProperties,
                    CanalConstants.CANAL_AUTO_SCAN_INTERVAL,
                    "5"));
                executor.scheduleWithFixedDelay(new Runnable() {

                    private PlainCanal lastCanalConfig;

                    public void run() {
                        try {
                            if (lastCanalConfig == null) {
                                lastCanalConfig = configClient.findServer(null);
                            } else {
                                PlainCanal newCanalConfig = configClient.findServer(lastCanalConfig.getMd5());
                                if (newCanalConfig != null) {
                                    // 远程配置canal.properties修改重新加载整个应用
                                    canalStater.stop();
                                    Properties managerProperties = newCanalConfig.getProperties();
                                    // merge local
                                    managerProperties.putAll(properties);
                                    canalStater.setProperties(managerProperties);
                                    canalStater.start();

                                    lastCanalConfig = newCanalConfig;
                                }
                            }

                        } catch (Throwable e) {
                            logger.error("scan failed", e);
                        }
                    }

                }, 0, scanIntervalInSecond, TimeUnit.SECONDS);
                canalStater.setProperties(managerProperties);
            } else {
                canalStater.setProperties(properties);
            }

            canalStater.start();
            /**
             * 通过CountDownLatch在此阻塞
             * 通过 Runtime.getRuntime().addShutdownHook(shutdownThread); 添加"关闭钩"实现优雅关闭
             */
            runningLatch.await();
            executor.shutdownNow();
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal Server:", e);
        }
    }

    private static void setGlobalUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> logger.error("UnCaughtException", e));
    }

}
