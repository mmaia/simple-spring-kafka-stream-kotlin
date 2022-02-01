package com.maia.simplespringkafkastreamkotlin.repository

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin.NewTopics
import java.util.*

@Configuration
@EnableKafka
@EnableKafkaStreams
class KafkaConfiguration {

    @Bean
    fun appTopics(): NewTopics {
        return NewTopics(
            TopicBuilder.name(STOCK_QUOTES_TOPIC).build(),
            TopicBuilder.name(LEVERAGE_PRICES_TOPIC).compact().build(),
        )
    }

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun defaultKafkaStreamsConfig(): KafkaStreamsConfiguration {
        val props: MutableMap<String, Any> = HashMap()
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = KAFKA_HOSTS
        props[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] =
            LogAndContinueExceptionHandler::class.java
        props[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = SCHEMA_REGISTRY_URL
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "quote-stream"
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = SpecificAvroSerde::class.java
        props[ConsumerConfig.GROUP_ID_CONFIG] = "stock-quotes-stream-group"

        return KafkaStreamsConfiguration(props)
    }

    @Bean
    fun configurer(): StreamsBuilderFactoryBeanConfigurer? {
        return StreamsBuilderFactoryBeanConfigurer { fb: StreamsBuilderFactoryBean ->
            fb.setStateListener { newState: KafkaStreams.State, oldState: KafkaStreams.State ->
                println("State transition from $oldState to $newState")
            }
        }
    }
}

// constants for topics
const val STOCK_QUOTES_TOPIC = "stock-quotes-topic"
const val LEVERAGE_PRICES_TOPIC = "leverage-prices-topic"
const val LEVERAGE_BY_SYMBOL_TABLE = "leverage-by-symbol-ktable"
val KAFKA_HOSTS: List<String> = listOf("localhost:9092")
const val SCHEMA_REGISTRY_URL = "http://localhost:8081"
