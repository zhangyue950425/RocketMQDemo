package com.zhangyue.rocketmq.client.baseSimple.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 单向发送消息示例：
 * 这种方式主要用在不特别关心发送结果的场景，例如日志发送。
 */
public class OnewayProducer {

    public static void main(String[] args) throws Exception{
        //实例化消息生产者Producer
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("producer_group_name3");
        //设置NameServer的地址
        defaultMQProducer.setNamesrvAddr("localhost:9876");

        //启动Producer实例
        defaultMQProducer.start();
        for (int i = 0; i < 100; i++) {
            Message message = new Message("TopicTest" /* topic */,
                    "TagA" /* tag */, ("Hello Rocket" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 发送单向消息，没有任何返回结果
            defaultMQProducer.sendOneway(message);
        }
        //如果不再发送消息，关闭Producer实例。
        defaultMQProducer.shutdown();
    }
}
