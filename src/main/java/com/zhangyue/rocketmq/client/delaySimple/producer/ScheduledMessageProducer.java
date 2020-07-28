package com.zhangyue.rocketmq.client.delaySimple.producer;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * 发送延时消息
 */
public class ScheduledMessageProducer {
    public static void main(String[] args) throws Exception{
        //实例化一个生产者来产生延时消息
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("ExampleProducerGroup");
        //启动生产者
        defaultMQProducer.start();
        int totalMessagesToSend = 100;
        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message("TopicTest", ("Hello scheduled message" + i).getBytes());
            //设置延时等级3,这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
            message.setDelayTimeLevel(3);
            //发送消息
            defaultMQProducer.send(message);
        }
        //关闭生产者
        defaultMQProducer.shutdown();

    }
}
