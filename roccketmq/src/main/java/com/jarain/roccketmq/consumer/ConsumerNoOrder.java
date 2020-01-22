package com.jarain.roccketmq.consumer;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.util.List;

@Slf4j
public class ConsumerNoOrder {

    public static void main(String[] args) {
        try {
            //创建消费者
            DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("No_Order_Group");
            // 设置nameserver
            defaultMQPushConsumer.setNamesrvAddr("192.168.6.145:9876");

            //设置消息拉去上限
            defaultMQPushConsumer.setConsumeMessageBatchMaxSize(3);
            //设置读取主题 tag
            defaultMQPushConsumer.subscribe("No_Order_Topic", "No_Order_Tag");
            // 设置消息监听
            defaultMQPushConsumer.setMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                    for (MessageExt messageExt : list) {
                        //messageExt 就是获取的消息
                        //获取消息主题、 tag、 body、、、、
                        String msgId = messageExt.getMsgId();
                        String topic = messageExt.getTopic();
                        String tags = messageExt.getTags();
                        try {
                            String body = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            log.info("获取的消息id:{}，主题:{}，tag:{}，body内容:{}", msgId, topic, tags, body);
                        } catch (UnsupportedEncodingException e) {
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            // 开始消费
            defaultMQPushConsumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
