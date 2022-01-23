package com.maia.simplespringkafkastreamkotlin.repository

import com.maia.springkafkastreamkotlin.repository.StockQuote
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Repository

@Repository
class StockQuoteProducer(val quoteProducer: KafkaTemplate<String, StockQuote>) {
    fun send(message: StockQuote) {
        quoteProducer.send(STOCK_QUOTES_TOPIC, message.symbol.toString(), message)
    }
}