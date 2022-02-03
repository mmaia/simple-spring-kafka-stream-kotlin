package com.maia.simplespringkafkastreamkotlin.api

import com.maia.simplespringkafkastreamkotlin.api.dto.LeveragePriceDTO
import com.maia.simplespringkafkastreamkotlin.api.dto.StockQuoteDTO
import com.maia.simplespringkafkastreamkotlin.repository.LeveragePriceProducer
import com.maia.simplespringkafkastreamkotlin.repository.LeverageStream
import com.maia.simplespringkafkastreamkotlin.repository.StockQuoteProducer
import com.maia.springkafkastreamkotlin.repository.LeveragePrice
import com.maia.springkafkastreamkotlin.repository.StockQuote
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.*
import java.math.BigDecimal

@Controller
@RequestMapping("api")
class QuotesController(
    val stockQuoteProducer: StockQuoteProducer,
    val leveragePriceProducer: LeveragePriceProducer,
    val leverageStream: LeverageStream
) {

    @PostMapping("/quotes")
    fun newQuote(@RequestBody stockQuoteDTO: StockQuoteDTO): ResponseEntity<StockQuoteDTO> {
        val stockQuote = StockQuote(
            stockQuoteDTO.symbol,
            stockQuoteDTO.tradeValue.toDouble(),
            stockQuoteDTO.isoDateTime.toEpochMilli()
        )
        stockQuoteProducer.send(stockQuote) // fire and forget
        return ResponseEntity.ok(stockQuoteDTO)
    }

    @PostMapping("/leverage")
    fun newLeveragePrice(@RequestBody leveragePriceDTO: LeveragePriceDTO): ResponseEntity<LeveragePriceDTO> {
        val leveragePrice = LeveragePrice(leveragePriceDTO.symbol, leveragePriceDTO.leverage.toDouble())
        leveragePriceProducer.send(leveragePrice) // fire and forget
        return ResponseEntity.ok(leveragePriceDTO)
    }

    @GetMapping("leveragePrice/{instrumentSymbol}")
    fun getLeveragePrice(@PathVariable instrumentSymbol: String): ResponseEntity<LeveragePriceDTO> {
        val leveragePrice: LeveragePrice = leverageStream.getLeveragePrice(instrumentSymbol)
            ?: return ResponseEntity.noContent().build()
        // if quote doesn't exist in our local store we return no content.
        val result = LeveragePriceDTO(leveragePrice.symbol.toString(), BigDecimal.valueOf(leveragePrice.leverage))
        return ResponseEntity.ok(result)
    }
}