package com.maia.simplespringkafkastreamkotlin.api.dto

import java.time.Instant

data class QuotesPerWindowDTO(val symbol: String, val count: Long, val start: Instant, val end: Instant)