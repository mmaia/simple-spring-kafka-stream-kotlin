package com.maia.simplespringkafkastreamkotlin.repository

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin.NewTopics

@Configuration
@EnableKafka
class KafkaConfiguration {

    @Bean
    fun appTopics(): NewTopics {
        return NewTopics(
            TopicBuilder.name(STOCK_QUOTES_TOPIC).build(),
            TopicBuilder.name(LEVERAGE_PRICES_TOPIC).compact().build(),
        )
    }
}

// constants for topics
const val STOCK_QUOTES_TOPIC = "stock-quotes-topic"
const val LEVERAGE_PRICES_TOPIC = "leverage-prices-topic"
