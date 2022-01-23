package com.maia.simplespringkafkastreamkotlin.api

import com.maia.simplespringkafkastreamkotlin.api.dto.LeveragePriceDTO
import com.maia.simplespringkafkastreamkotlin.api.dto.StockQuoteDTO
import com.maia.simplespringkafkastreamkotlin.repository.LeveragePriceProducer
import com.maia.simplespringkafkastreamkotlin.repository.StockQuoteProducer
import com.maia.springkafkastreamkotlin.repository.LeveragePrice
import com.maia.springkafkastreamkotlin.repository.StockQuote
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping

@Controller
@RequestMapping("api")
class QuotesController(val stockQuoteProducer: StockQuoteProducer, val leveragePriceProducer: LeveragePriceProducer) {

    @PostMapping("/quotes")
    fun newQuote(@RequestBody stockQuoteDTO: StockQuoteDTO): ResponseEntity<StockQuoteDTO> {
        val stockQuote = StockQuote(stockQuoteDTO.symbol, stockQuoteDTO.tradeValue.toDouble(), stockQuoteDTO.isoDateTime.toEpochMilli())
        stockQuoteProducer.send(stockQuote) // fire and forget
        return ResponseEntity.ok(stockQuoteDTO)
    }

    @PostMapping("/leverage")
    fun newLeveragePrice(@RequestBody leveragePriceDTO: LeveragePriceDTO): ResponseEntity<LeveragePriceDTO> {
        val leveragePrice = LeveragePrice(leveragePriceDTO.symbol, leveragePriceDTO.leverage.toDouble())
        leveragePriceProducer.send(leveragePrice) // fire and forget
        return ResponseEntity.ok(leveragePriceDTO)
    }
}