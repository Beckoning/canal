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
    private static ScheduledExecutorService executor             = Executors.newScheduledThreadPool(1,
                                                                     new NamedThreadFactory("canal-server-scan"));


    /**
     * 问题：
     * 1、CanalServer启动过程中配置如何加载？
     *      如果使用admin配置，启动一个线程池每隔一段时间（默认5秒）拉去并更新配置，如果使用的不是admin配置直接获取canal.properties配置信息
     *
     * 2、CanalServer启动过程中涉及哪些组件？
     *      2、1 CanalController：是canalServer真正的启动控制器
     *      2、2 canalMQStarter  用来启动mqProducer。如果serverMode选择了mq，那么会用canalMQStarter来管理mqProducer，将canalServer抓取到的实时变更用mqProducer直接投递到mq
     *      2、3 CanalAdminWithNetty  这个不是admin控制台，而是对本server启动一个netty服务，让admin控制台通过请求获取当前server的信息，比如运行状态、正在本server上运行的instance信息等
     *
     * 3、集群模式的canalServer，是如何实现instance的HA呢？（利用zkClient监听canal instance在zookeeper上的状态变化，动态停止、启动或新增，实现了instance的HA）
     *      3、1 为每一个instance创建一个ServerRunningMonitor 用来监控服务状态,并配置一个监听器（ServerRunningListener）
     *      3、2 ServerRunningMonitor启动的时候
     *              如果zkClient!=null 使用zk HA  如果节点成功 就启动ServerRunningListener.processActiveEnter 启动CanalServerWithEmbedded（保证只有一个使用）
     *              如果zkClient==null 则直接ServerRunningListener.processActiveEnter 启动CanalServerWithEmbedded
     *      3.3 如果zk数据发送变化  通过监听机制设置instance运行实例

     * 4、每个canalInstance又是怎么获取admin上的配置变更呢？
     *
     *
     * @param args
     */

    public static void main(String[] args) {
        try {
            logger.info("## set default uncaught exception handler");
            //note:设置全局未捕获异常的处理
            setGlobalUncaughtExceptionHandler();

            logger.info("## load canal configurations");
            //读取canal.properties的配置
            String conf = System.getProperty("canal.conf", "classpath:canal.properties");
            Properties properties = new Properties();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                properties.load(CanalLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                properties.load(new FileInputStream(conf));
            }

            final CanalStarter canalStater = new CanalStarter(properties);
            //根据canal.admin.manager是否为空判断是否是admin控制,如果不是admin控制，就直接根据canal.properties的配置来了
            String managerAddress = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
            if (StringUtils.isNotEmpty(managerAddress)) {
                String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
                String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
                String adminPort = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT, "11110");
                boolean autoRegister = BooleanUtils.toBoolean(CanalController.getProperty(properties,
                    CanalConstants.CANAL_ADMIN_AUTO_REGISTER));
                String autoCluster = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_AUTO_CLUSTER);
                String registerIp = CanalController.getProperty(properties, CanalConstants.CANAL_REGISTER_IP);
                if (StringUtils.isEmpty(registerIp)) {
                    registerIp = AddressUtils.getHostIp();
                }
                //如果是admin控制，使用PlainCanalConfigClient获取远程配置 新开一个线程池每隔五秒用http请求去admin上拉配置进行merge 用md5进行校验，如果canal-server配置有更新，那么就重启canal-server
                final PlainCanalConfigClient configClient = new PlainCanalConfigClient(managerAddress,
                    user,
                    passwd,
                    registerIp,
                    Integer.parseInt(adminPort),
                    autoRegister,
                    autoCluster);
                PlainCanal canalConfig = configClient.findServer(null);
                if (canalConfig == null) {
                    throw new IllegalArgumentException("managerAddress:" + managerAddress
                                                       + " can't not found config for [" + registerIp + ":" + adminPort
                                                       + "]");
                }
                Properties managerProperties = canalConfig.getProperties();
                // merge local
                managerProperties.putAll(properties);
                int scanIntervalInSecond = Integer.valueOf(CanalController.getProperty(managerProperties,
                    CanalConstants.CANAL_AUTO_SCAN_INTERVAL,
                    "5"));
                //新开一个线程池每隔五秒用http请求去admin上拉配置进行merge（这里依赖了instance模块的相关配置拉取的工具方法）
                executor.scheduleWithFixedDelay(new Runnable() {

                    private PlainCanal lastCanalConfig;

                    public void run() {
                        try {
                            if (lastCanalConfig == null) {
                                lastCanalConfig = configClient.findServer(null);
                            } else {
                                //用md5进行校验，如果canal-server配置有更新，那么就重启canal-server
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
            //核心是用canalStarter.start()启动
            canalStater.start();
            //使用CountDownLatch保持主线程存活
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
