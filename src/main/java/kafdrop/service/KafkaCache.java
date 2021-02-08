package kafdrop.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import kafdrop.model.TopicPartitionVO;
import kafdrop.model.TopicVO;
import org.springframework.stereotype.Service;

@Service
public class KafkaCache {

  private final KafkaHighLevelAdminClient highLevelAdminClient;
  private final KafkaHighLevelConsumer highLevelConsumer;
  private Cache<String, KafkaHighLevelAdminClient.ClusterDescription> kafkaBrokerCache;
  private Cache<String, Map<String, TopicVO>> kafkaTopicCache;
  private Cache<String, Map<Integer, TopicPartitionVO>> kafkaTopicPartitionCache;


  public KafkaCache(KafkaHighLevelAdminClient highLevelAdminClient,
                    KafkaHighLevelConsumer highLevelConsumer) {
    this.highLevelAdminClient = highLevelAdminClient;
    this.highLevelConsumer = highLevelConsumer;
  }

  @PostConstruct
  public void init() {
    kafkaBrokerCache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .expireAfterAccess(180, TimeUnit.MINUTES)
        .build();
    kafkaTopicCache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .expireAfterAccess(180, TimeUnit.MINUTES)
        .build();
    kafkaTopicPartitionCache = CacheBuilder.newBuilder()
        .maximumSize(100)
        .expireAfterAccess(180, TimeUnit.MINUTES)
        .build();
  }

  public KafkaHighLevelAdminClient.ClusterDescription describeCluster() {
    try {
      return kafkaBrokerCache.get("KAFKA_BROKERS", () -> highLevelAdminClient.describeCluster());
    } catch (ExecutionException e) {
      throw new KafkaAdminClientException(e);
    }
  }

  public Map<String, TopicVO> getTopicInfos(String[] topics) {
    try {
      return kafkaTopicCache.get("TOPICS", () -> highLevelConsumer.getTopicInfos(topics));
    } catch (ExecutionException e) {
      throw new KafkaAdminClientException(e);
    }
  }

  public Map<Integer, TopicPartitionVO> getPartitionSize(String topicName) {
    try {
      return kafkaTopicPartitionCache.get(topicName, () -> highLevelConsumer.getPartitionSize(topicName));
    } catch (ExecutionException e) {
      throw new KafkaAdminClientException(e);
    }
  }

}
