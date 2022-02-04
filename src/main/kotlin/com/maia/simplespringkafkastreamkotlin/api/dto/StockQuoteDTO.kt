package com.maia.simplespringkafkastreamkotlin.api.dto

import com.fasterxml.jackson.annotation.JsonFormat
import java.math.BigDecimal
import java.time.Instant

data class StockQuoteDTO(val symbol: String, val tradeValue: BigDecimal,
                    @JsonFormat(shape = JsonFormat.Shape.STRING, timezone = "UTC") val isoDateTime: Instant)