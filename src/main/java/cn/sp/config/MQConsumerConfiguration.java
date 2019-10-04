package cn.sp.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Created by 2YSP on 2019/10/4.
 */
@Slf4j
@Component
@PropertySource("classpath:/rocketmq.properties")
@ConfigurationProperties(prefix = "rocketmq.consumer")
@Data
public class MQConsumerConfiguration {

  private String groupName;

  private String namesrvAddr;
  /**
   * 该消费者订阅的主题和tags("*"号表示订阅该主题下所有的tags),
   * 格式：topic~tag1||tag2||tag3;topic2~*
   */
  private String topics;

  private Integer consumeThreadMin;

  private Integer consumeThreadMax;

  private Integer consumeMessageBatchMaxSize;

  @Autowired
  private MQConsumerMsgListener consumerMsgListener;

  @Bean
  @ConditionalOnProperty(name = "rocketmq.consumer.isOnOff", havingValue = "on")
  public DefaultMQPushConsumer getMQConsumer() {
    if (StringUtils.isBlank(groupName)) {
      throw new IllegalArgumentException("groupName is null!");
    }
    if (StringUtils.isBlank(namesrvAddr)) {
      throw new IllegalArgumentException("namesrvAddr is null!");
    }
    if (StringUtils.isBlank(topics)) {
      throw new IllegalArgumentException("topics is null!");
    }
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
    consumer.setNamesrvAddr(namesrvAddr);
    consumer.setConsumeThreadMin(consumeThreadMin);
    consumer.setConsumeThreadMax(consumeThreadMax);
    /**
     *  设置消费者第一次启动是从队列头部开始消费还是从队列尾部开始消费
     *  如果非第一次启动，那么按照上次消费的位置继续消费
     */
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
    /**
     * 设置消费模型，集群还是广播，默认为集群
     */
    //consumer.setMessageModel(MessageModel.CLUSTERING);
    /**
     * 设置一次消费消息的条数，默认为1条
     */
    consumer.setConsumeMessageBatchMaxSize(consumeMessageBatchMaxSize);
    try {
      String[] topicTagsArr = topics.split(";");
      for (String topicTags : topicTagsArr) {
        String[] topicTag = topicTags.split("~");
        consumer.subscribe(topicTag[0], topicTag[1]);
      }
      consumer.registerMessageListener(consumerMsgListener);
      consumer.start();
      log.info("consumer is start !!! groupName:{},topics:{},namesrvAddr:{}",groupName,topics,namesrvAddr);
    } catch (MQClientException e) {
      log.error("consumer is start !!! groupName:{},topics:{},namesrvAddr:{}",groupName,topics,namesrvAddr,e);
      e.printStackTrace();
    }
    return consumer;
  }

}
