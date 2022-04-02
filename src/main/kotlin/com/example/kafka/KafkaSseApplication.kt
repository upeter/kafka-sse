package com.example.kafka

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaAdmin

@SpringBootApplication
class KafkaSseApplication

fun main(args: Array<String>) {
	runApplication<KafkaSseApplication>(*args)
}

@Configuration
class KafkaConfig {
	@Bean
	open fun kafkaAdmin(): KafkaAdmin =
		KafkaAdmin(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9093", CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG to "100000"))

	@Bean
	fun kafkaAdminClient(kafkaAdmin: KafkaAdmin): AdminClient {
		return AdminClient.create(kafkaAdmin.configurationProperties)
	}

}
