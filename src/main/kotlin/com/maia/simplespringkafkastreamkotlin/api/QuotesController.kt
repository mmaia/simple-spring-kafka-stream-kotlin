package com.maia.simplespringkafkastreamkotlin.api

import com.maia.simplespringkafkastreamkotlin.api.dto.LeveragePriceDTO
import com.maia.simplespringkafkastreamkotlin.api.dto.QuotesPerWindowDTO
import com.maia.simplespringkafkastreamkotlin.api.dto.StockQuoteDTO
import com.maia.simplespringkafkastreamkotlin.repository.LeveragePriceProducer
import com.maia.simplespringkafkastreamkotlin.repository.LeverageStream
import com.maia.simplespringkafkastreamkotlin.repository.QuoteStream
import com.maia.simplespringkafkastreamkotlin.repository.StockQuoteProducer
import com.maia.springkafkastream.repository.QuotesPerWindow
import com.maia.springkafkastreamkotlin.repository.LeveragePrice
import com.maia.springkafkastreamkotlin.repository.StockQuote
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.*
import java.math.BigDecimal
import java.time.Instant

@Controller
@RequestMapping("api")
class QuotesController(
    val stockQuoteProducer: StockQuoteProducer,
    val leveragePriceProducer: LeveragePriceProducer,
    val leverageStream: LeverageStream,
    val quoteStream: QuoteStream
) {

    @PostMapping("/quotes")
    fun newQuote(@RequestBody stockQuoteDTO: StockQuoteDTO): ResponseEntity<StockQuoteDTO> {
        val (symbol, tradeValue, isoDateTime) = stockQuoteDTO
        val stockQuote = StockQuote(
            symbol,
            tradeValue.toDouble(),
            isoDateTime.toEpochMilli())
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

    @GetMapping("/quotes/count/{symbol}")
    fun getQuotesPerWindow(
        @PathVariable symbol: String,
        @RequestParam("pastMinutes") pastMinutes: Int
    ): ResponseEntity<QuotesPerWindowDTO> {
        val end = Instant.now()
        val start = end.minusSeconds(pastMinutes.toLong() * 60)
        val quotesPerWindow: QuotesPerWindow = quoteStream.quoteVolumePerInterval(symbol, start.toEpochMilli(), end.toEpochMilli())
        val result = QuotesPerWindowDTO(symbol, quotesPerWindow.count, start, end)
        return ResponseEntity.ok(result)
    }
}