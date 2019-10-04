package cn.sp;

import org.apache.rocketmq.client.consumer.MQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootRocketmqApplicationTests {

	@Autowired
	MQPushConsumer consumer;

	@Autowired
	DefaultMQProducer producer;

	@Test
	public void contextLoads() {
	}


	@Test
	public void send() throws  Exception{
		Message msg = new Message("DemoTopic", "TagA",
				"hello my".getBytes(RemotingHelper.DEFAULT_CHARSET));
		SendResult sendResult = producer.send(msg);
		System.out.println("发送结果："+sendResult.toString());

	}

}
