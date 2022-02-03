package com.maia.simplespringkafkastreamkotlin.repository

import com.maia.springkafkastreamkotlin.repository.LeveragePrice
import com.maia.springkafkastreamkotlin.repository.ProcessedQuote
import com.maia.springkafkastreamkotlin.repository.StockQuote
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.kstream.KStream
import org.springframework.context.annotation.Bean
import org.springframework.kafka.support.KafkaStreamBrancher
import org.springframework.stereotype.Repository

@Repository
class QuoteStream(val leveragepriceGKTable: GlobalKTable<String, LeveragePrice>) {

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
            .branch({ symbolKey, _ -> symbolKey.equals("GOOGL",ignoreCase = true) }, { ks -> ks.to(GOOGL_STOCKS_TOPIC) })
            .defaultBranch { ks -> ks.to(ALL_OTHER_STOCKS_TOPIC) }
            .onTopOf(resStream)

        return resStream
    }

}