package com.alibaba.otter.canal.server;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.server.exception.CanalServerException;

/**
 * 对应canal整个服务实例，一个jvm实例只有一份server
 *
 * server代表了一个canal 的运行实例，为了方便组件化使用，特意抽象了Embeded（嵌入式）/Netty（网络访问）两种实现方式
 *    Embeded：对latency和可用性都有比较高的要求，自己又能hold住分布式的相关技术
 *    Netty：基于netty封装了一层网络协议，由canal server保证其可用性，采用pull模型，当然latency会略微打点折扣
 * 
 * @author jianghang 2012-7-12 下午01:32:29
 * @version 1.0.0
 */
public interface CanalServer extends CanalLifeCycle {

    void start() throws CanalServerException;

    void stop() throws CanalServerException;
}
