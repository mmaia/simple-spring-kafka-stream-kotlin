package com.maia.simplespringkafkastreamkotlin.repository

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin.NewTopics

@Configuration
@EnableKafka
class KafkaConfiguration {

    // TOPICS
    val quotesTopic = "stock-quotes-topic"
    val leveragePriceTopic = "leverage-prices-topic"

    @Bean
    fun appTopics(): NewTopics? {
        return NewTopics(
            TopicBuilder.name(quotesTopic).build(),
            TopicBuilder.name(leveragePriceTopic).compact().build(),
        )
    }
}