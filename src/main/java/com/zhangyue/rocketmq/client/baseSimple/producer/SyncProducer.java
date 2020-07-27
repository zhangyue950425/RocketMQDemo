package com.zhangyue.rocketmq.client.baseSimple.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * 示例：生产者同步发送消息
 * 这种可靠性同步地发送方式使用的比较广泛，比如：重要的消息通知，短信通知。
 */
public class SyncProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //实例化消息生产者Producer
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("producer_group_name1");

        //设置NameServer的地址
        defaultMQProducer.setNamesrvAddr("localhost:9876");

        //启动Producer实例
        defaultMQProducer.start();

        for (int i = 0; i < 100; i++) {
            //创建消息，并指定Topic，Tag和消息体
            Message message = new Message("TopicTest" /* Topic*/,  "TagA" /* tag*/,
                    ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message Body*/);

            //发送消息到一个Broker
            SendResult sendResult = defaultMQProducer.send(message);

            //通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
        }

        //如果不再发送消息，关闭Producer实例。
        defaultMQProducer.shutdown();
    }
}
