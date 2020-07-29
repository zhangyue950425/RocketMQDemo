package com.zhangyue.rocketmq.client.delaySimple.consumer;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 延时消息样例：
 * 启动消费者等待传入订阅消息
 */
public class ScheduledMessageConsumer {

    public static void main(String[] args) throws Exception{
        // 实例化消费者
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("ExampleConsumer");
        //订阅Topics
        defaultMQPushConsumer.subscribe("TopicTest", "*");
        //注册消息监听者
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    // Print approximate delay time period
                    System.out.println("Receive message[msgId=" + messageExt.getMsgId() + "] " + (System.currentTimeMillis() - messageExt.getStoreTimestamp()) + "ms later");
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动消费者
        defaultMQPushConsumer.start();
    }
}
