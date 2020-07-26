package com.zhangyue.rocketmq.client.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;

/**
 * 生产者发送异步消息示例：
 * 异步消息通常用在对响应时间敏感的业务场景，即发送端不能容忍长时间地等待Broker的响应。
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception{
        //实例化消息生产者Producer
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("producer_group_name2");

        //设置NameServer的地址
        defaultMQProducer.setNamesrvAddr("localhost:9876");

        //启动Producer实例
        defaultMQProducer.start();

        //
        defaultMQProducer.setRetryTimesWhenSendAsyncFailed(0);

        int messageCount = 100;
        //根据消息数量实例化倒计时计算器
        final CountDownLatch2 countDownLatch2 = new CountDownLatch2(messageCount);
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            //创建消息，并指定Topic，Tag和消息体
            Message message = new Message("TopicTest",
                    "TagA",
                    "OrderID188",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

            //SendCallback接收异步返回结果的回调
            defaultMQProducer.send(message, new SendCallback() {
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                }

                public void onException(Throwable throwable) {
                    System.out.printf("%-10d Exception %s %n", index, throwable);
                    throwable.printStackTrace();
                }
            });
        }
        //等待5s
        countDownLatch2.await(5, TimeUnit.SECONDS);
        //如果不再发送消息，关闭Producer实例。
        defaultMQProducer.shutdown();
    }
}
