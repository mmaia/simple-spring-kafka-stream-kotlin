package com.maia.simplespringkafkastreamkotlin.repository

import com.maia.springkafkastreamkotlin.repository.LeveragePrice
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Repository

@Repository
class LeveragePriceProducer(val leveragePriceProducer: KafkaTemplate<String, LeveragePrice> ) {
    fun send(message: LeveragePrice) {
        leveragePriceProducer.send(LEVERAGE_PRICES_TOPIC, message.symbol.toString(), message)
    }
}