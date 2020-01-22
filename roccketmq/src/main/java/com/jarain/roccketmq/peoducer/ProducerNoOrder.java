package com.jarain.roccketmq.peoducer;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.Message;

public class ProducerNoOrder {
    public static void main(String[] args) {

        try {
            //创建消息发送者
            DefaultMQProducer defaultMQProducer = new DefaultMQProducer("No_Order_Group");
            //设置nameserver
            defaultMQProducer.setNamesrvAddr("192.168.6.145:9876");
            //开启发送者
            defaultMQProducer.start();
            for (int i = 10; i < 100; i++) {
                //创建消息  String topic, String tags, String keys, byte[] body
                Message message = new Message("No_Order_Topic", "No_Order_Tag", "keys" + i, ("hello 我是msg" + i).getBytes());
                System.out.println(message);
                //发送
                defaultMQProducer.send(message);
            }
            defaultMQProducer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
