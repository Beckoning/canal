package com.alibaba.otter.canal.instance.core;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;

/**
 * 代表单个canal实例，比如一个destination会独立一个实例
 * 
 * @author jianghang 2012-7-12 下午12:04:58
 * @version 1.0.0
 */
public interface CanalInstance extends CanalLifeCycle {

    /**
     * instance对应的destination
     * @return
     */
    String getDestination();

    /**
     * 数据源接入，模拟slave协议和master协议进行交互，协议解析位于 canal.parse模块
     * @return
     */
    CanalEventParser getEventParser();

    /**
     * Parser和storel连接器，进行数据过滤、加工、分发工作
     * @return
     */
    CanalEventSink getEventSink();

    /**
     * 获取存储数据的eventstore
     * @return
     */
    CanalEventStore getEventStore();

    /**
     * 获取增量订阅&消费消息管理器
     * @return
     */
    CanalMetaManager getMetaManager();

    /**
     * 获取告警 位于canal.conmmon模块中
     * @return
     */
    CanalAlarmHandler getAlarmHandler();

    /**
     * 客户端发生订阅/取消订阅行为
     */
    boolean subscribeChange(ClientIdentity identity);

    /**
     * 相关mq的配置信息，用作mqProvider的入参
     * @return
     */
    CanalMQConfig getMqConfig();
}
