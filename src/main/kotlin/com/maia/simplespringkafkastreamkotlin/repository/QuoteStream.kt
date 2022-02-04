package com.maia.simplespringkafkastreamkotlin.repository

import com.maia.springkafkastream.repository.QuotesPerWindow
import com.maia.springkafkastreamkotlin.repository.LeveragePrice
import com.maia.springkafkastreamkotlin.repository.ProcessedQuote
import com.maia.springkafkastreamkotlin.repository.StockQuote
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.*
import org.springframework.context.annotation.Bean
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.kafka.support.KafkaStreamBrancher
import org.springframework.stereotype.Repository
import java.time.Duration
import java.time.Instant
import java.util.function.BiFunction
import javax.annotation.PostConstruct

@Repository
class QuoteStream(val leveragepriceGKTable: GlobalKTable<String, LeveragePrice>) {

    private val quotesPerWindowSerde: SpecificAvroSerde<QuotesPerWindow> = SpecificAvroSerde<QuotesPerWindow>()
    private lateinit var quotesPerWindowView: ReadOnlyWindowStore<String, Long>

    /**
     * From processed quote to quotes per window
     */
    var processedQuoteToQuotesPerWindowMapper =
        BiFunction<Windowed<String>, Long, KeyValue<String, QuotesPerWindow>> { key, value ->
            val quotesPerWindow = QuotesPerWindow(key.key(), key.window().start(), key.window().end(), value ?: 0)
            KeyValue(key.key(), quotesPerWindow)
        }


    @PostConstruct
    fun init() {
        quotesPerWindowSerde.configure(serdeConfig, false)
    }

    @Bean
    fun afterStartQuote(sbfb: StreamsBuilderFactoryBean): StreamsBuilderFactoryBean.Listener {
        val listener: StreamsBuilderFactoryBean.Listener = object : StreamsBuilderFactoryBean.Listener {
            override fun streamsAdded(id: String, streams: KafkaStreams) {
                quotesPerWindowView = streams
                    .store(
                        StoreQueryParameters
                            .fromNameAndType(QUOTES_BY_WINDOW_TABLE, QueryableStoreTypes.windowStore())
                    )
            }
        }
        sbfb.addListener(listener)
        return listener
    }

    @Bean
    fun quoteKStream(streamsBuilder: StreamsBuilder): KStream<String, ProcessedQuote> {
        val stream: KStream<String, StockQuote> = streamsBuilder.stream(STOCK_QUOTES_TOPIC)

        val resStream: KStream<String, ProcessedQuote> = stream
            .leftJoin(leveragepriceGKTable,
                { symbol, _ -> symbol },
                { stockQuote, leveragePrice ->
                    ProcessedQuote(
                        stockQuote.symbol,
                        stockQuote.tradeValue,
                        stockQuote.tradeTime,
                        leveragePrice?.leverage
                    )
                }
            )

        // branches it and pushes to proper topics
        KafkaStreamBrancher<String, ProcessedQuote>()
            .branch({ symbolKey, _ -> symbolKey.equals("APPL", ignoreCase = true) }, { ks -> ks.to(AAPL_STOCKS_TOPIC) })
            .branch({ symbolKey, _ -> symbolKey.equals("GOOGL", ignoreCase = true) }, { ks -> ks.to(GOOGL_STOCKS_TOPIC) })
            .defaultBranch { ks -> ks.to(ALL_OTHER_STOCKS_TOPIC) }
            .onTopOf(resStream)

        // group by key
        val groupedBySymbol: KGroupedStream<String, ProcessedQuote> = resStream.groupByKey()

        // count and send to compact topic so we can always quickly access the total count by symbol
        val quotesCount: KTable<String, Long> = groupedBySymbol.count()
        quotesCount.toStream().to(COUNT_TOTAL_QUOTES_BY_SYMBOL_TOPIC, Produced.with(Serdes.String(), Serdes.Long()))

        // we could transform it materialize it to be able to query directly from kafka
        val quotesWindowedKTable: KTable<Windowed<String>, Long> = groupedBySymbol
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
            .count(
                Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>(QUOTES_BY_WINDOW_TABLE)
                    .withValueSerde(Serdes.Long())
            )

        // and then transform and send it to a new topic which we could then use a connector and send to elastic or mongodb for example...
        quotesWindowedKTable
            .toStream()
            .map(processedQuoteToQuotesPerWindowMapper::apply)
            .to(COUNT_WINDOW_QUOTES_BY_SYMBOL_TOPIC, Produced.with(Serdes.String(), quotesPerWindowSerde))

        return resStream
    }

    fun quoteVolumePerInterval(key: String, start: Long, end: Long): QuotesPerWindow {
        val listOfCountedQuotes: WindowStoreIterator<Long> =
            quotesPerWindowView.fetch(key, Instant.ofEpochMilli(start), Instant.ofEpochMilli(end))

        var totalCountForInterval = 0L
        listOfCountedQuotes.forEach {
            totalCountForInterval += it.value
        }

        return QuotesPerWindow(key, start, end, totalCountForInterval)
    }

}