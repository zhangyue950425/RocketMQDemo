package com.zhangyue.rocketmq.client.baseSimple.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 消费者：消费消息
 */
public class Consumer {
    public static void main(String[] args) throws Exception{
        //实例化消费者
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("consumer_group_name");
        //设置NameServer的地址
        defaultMQPushConsumer.setNamesrvAddr("localhost:9876");
        //订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        defaultMQPushConsumer.subscribe("TopicTest", "*");
        //注册回调实现类来处理从broker拉取回来的消息
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), list);
                //标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动消费者实例
        defaultMQPushConsumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
