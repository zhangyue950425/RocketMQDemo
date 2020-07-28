package com.zhangyue.rocketmq.client.transaction.listener;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 实现事务的监听接口:
 * 当发送半消息成功时，我们使用 executeLocalTransaction 方法来执行本地事务。
 * checkLocalTransaction 方法用于检查本地事务状态，并回应消息队列的检查请求。
 */
public class TransactionListenerImpl implements TransactionListener {
    private AtomicInteger atomicInteger = new AtomicInteger(0);
    private ConcurrentHashMap<String, Integer> concurrentHashMap = new ConcurrentHashMap();
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        int value = atomicInteger.getAndIncrement();
        int status = value % 3;
        concurrentHashMap.put(message.getTransactionId(), status);
        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        Integer status = concurrentHashMap.get(messageExt.getTransactionId());
        if (status != null) {
            switch (status) {
                case 0:
                    return LocalTransactionState.UNKNOW;
                case 1:
                    return LocalTransactionState.COMMIT_MESSAGE;
                case 2:
                    return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
