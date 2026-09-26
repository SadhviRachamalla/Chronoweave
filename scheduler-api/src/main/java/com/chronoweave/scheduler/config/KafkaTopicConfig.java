package com.chronoweave.scheduler.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic dispatchTopic() {
        return TopicBuilder.name("chronoweave-dispatch")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic statusTopic() {
        return TopicBuilder.name("chronoweave-status")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name("chronoweave-dlq")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
