package com.zhangyue.rocketmq.client.transaction.producer;

import com.zhangyue.rocketmq.client.transaction.listener.TransactionListenerImpl;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * 发送事务消息样例:
 * 1、创建事务性生产者
 * 使用 @TransactionMQProducer 类创建生产者，
 * 并指定唯一的 ProducerGroup，就可以设置自定义线程池来处理这些检查请求。
 * 执行本地事务后、需要根据执行结果对消息队列进行回复。
 */
public class TransactionProducer {

    public static void main(String[] args) throws Exception{
        TransactionListener transactionListener = new TransactionListenerImpl();
        TransactionMQProducer transactionMQProducer = new TransactionMQProducer("please_rename_unique_group_name");
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
        transactionMQProducer.setExecutorService(executorService);
        transactionMQProducer.setTransactionListener(transactionListener);
        transactionMQProducer.start();
        String[] tags = new String[]{"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            try {
                Message message = new Message("TopicTest1234", tags[i % tags.length],
                        "KEY" + i, ("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = transactionMQProducer.sendMessageInTransaction(message, null);
                System.out.printf("%s%n", sendResult);
                Thread.sleep(10);
            } catch (MQClientException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }
        transactionMQProducer.shutdown();
    }
}
