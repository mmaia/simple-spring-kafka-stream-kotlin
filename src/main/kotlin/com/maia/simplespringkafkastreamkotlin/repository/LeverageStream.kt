package com.maia.simplespringkafkastreamkotlin.repository

import com.maia.springkafkastreamkotlin.repository.LeveragePrice
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.context.annotation.Bean
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.stereotype.Repository
import java.util.*
import javax.annotation.PostConstruct

@Repository
class LeverageStream {

    private val leveragePriceSerde = SpecificAvroSerde<LeveragePrice>()
    private lateinit var leveragePriceView: ReadOnlyKeyValueStore<String, LeveragePrice>

    @PostConstruct
    fun init() {
        leveragePriceSerde.configure(serdeConfig, false)
    }

    @Bean
    fun afterStartLeverage(sbfb: StreamsBuilderFactoryBean): StreamsBuilderFactoryBean.Listener {
        val listener: StreamsBuilderFactoryBean.Listener = object : StreamsBuilderFactoryBean.Listener {
            override fun streamsAdded(id: String, streams: KafkaStreams) {
                leveragePriceView = streams.store<ReadOnlyKeyValueStore<String, LeveragePrice>>(
                    StoreQueryParameters.fromNameAndType(
                        LEVERAGE_BY_SYMBOL_TABLE,
                        QueryableStoreTypes.keyValueStore()
                    )
                )
            }
        }

        sbfb.addListener(listener)
        return listener
    }

    @Bean
    fun leveragePriceBySymbolGKTable(streamsBuilder: StreamsBuilder): GlobalKTable<String, LeveragePrice> {
        return streamsBuilder
            .globalTable(
                LEVERAGE_PRICES_TOPIC,
                Materialized.`as`<String, LeveragePrice, KeyValueStore<Bytes, ByteArray>>(LEVERAGE_BY_SYMBOL_TABLE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(leveragePriceSerde)
            )
    }

    fun getLeveragePrice(key: String?): LeveragePrice? {
        return leveragePriceView[key]
    }
}