package com.maia.simplespringkafkastreamkotlin

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class SimpleSpringKafkaStreamKotlinApplication

fun main(args: Array<String>) {
	runApplication<SimpleSpringKafkaStreamKotlinApplication>(*args)
}
