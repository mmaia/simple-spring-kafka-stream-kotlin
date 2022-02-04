package com.maia.simplespringkafkastreamkotlin.api.dto

import java.math.BigDecimal

data class LeveragePriceDTO(val symbol: String, val leverage: BigDecimal)