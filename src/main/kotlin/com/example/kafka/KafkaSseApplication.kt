package com.example.kafka

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaSseApplication

fun main(args: Array<String>) {
	runApplication<KafkaSseApplication>(*args)
}
