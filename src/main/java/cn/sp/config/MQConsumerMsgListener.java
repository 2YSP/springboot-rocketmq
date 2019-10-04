package cn.sp.config;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * Created by 2YSP on 2019/10/4.
 */
@Slf4j
@Component
public class MQConsumerMsgListener implements MessageListenerConcurrently {

  /**
   * 默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息
   * 不要抛异常，如果没有 return CONSUME_SUCCESS,consumer 会重新消费该消息，直到return CONSUME_SUCCESS
   * @param msgs
   * @param context
   * @return
   */
  @Override
  public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
      ConsumeConcurrentlyContext context) {
    if (CollectionUtils.isEmpty(msgs)){
        log.warn("接收到的消息为空，不处理返回成功");
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
    MessageExt message = msgs.get(0);
    log.info("接收到的消息为："+message.toString());
    if ("DemoTopic".equals(message.getTopic())){
      //TODO 判断该消息是否重复消费（RocketMQ不保证消息不重复，如果你的业务需要保证严格的不重复消息，需要你自己在业务端去重）
      //TODO 获取该消息重试次数
      int reconsumeTimes = message.getReconsumeTimes();
      if(reconsumeTimes ==3){//消息已经重试了3次，如果不需要再次消费，则返回成功
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
      }
      //TODO 处理对应的业务逻辑
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  }
}
