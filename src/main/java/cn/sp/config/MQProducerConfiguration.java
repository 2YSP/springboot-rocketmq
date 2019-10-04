package cn.sp.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * Created by 2YSP on 2019/10/4.
 */
@Slf4j
@Component
@PropertySource("classpath:/rocketmq.properties")
@ConfigurationProperties(prefix = "rocketmq.producer")
@Data
public class MQProducerConfiguration {

  private String groupName;
  /**
   * mq的nameserver地址
   */
  private String namesrvAddr;
  /**
   * 消息最大长度 默认1024*4(4M)
   */
  private Integer maxMessageSize;
  /**
   * 发送消息超时时间,默认3000
   */
  private Integer sendMsgTimeout;
  /**
   * 发送消息失败重试次数，默认2
   */
  private Integer retryTimesWhenSendFailed;

  @Bean
  @ConditionalOnProperty(name = "rocketmq.producer.isOnOff",havingValue = "on")
  public DefaultMQProducer getDefaultMQProducer() {
    DefaultMQProducer producer = new DefaultMQProducer(this.groupName);
    producer.setNamesrvAddr(namesrvAddr);
    //如果需要同一个jvm中不同的producer往不同的mq集群发送消息，需要设置不同的instanceName
    //producer.setInstanceName(instanceName);
    if (this.maxMessageSize != null){
      producer.setMaxMessageSize(this.maxMessageSize);
    }
    if (this.sendMsgTimeout != null){
      producer.setSendMsgTimeout(this.sendMsgTimeout);
    }

    if (this.retryTimesWhenSendFailed != null){
      producer.setRetryTimesWhenSendFailed(retryTimesWhenSendFailed);
    }

    try {
      producer.start();
      log.info(String.format("producer is start ! groupName:[%s],namesrvAddr:[%s]"
          , this.groupName, this.namesrvAddr));
    } catch (MQClientException e) {
      e.printStackTrace();
    }

    return producer;
  }
}
