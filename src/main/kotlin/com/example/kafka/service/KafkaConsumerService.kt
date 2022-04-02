package com.example.kafka.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.util.*
import javax.annotation.PostConstruct


@Component
class KafkaConsumerService {
    private val kafkaReceiver: KafkaReceiver<Long, String>

    init {
        val consumerProps = mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to IntegerDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
//            ConsumerConfig.CLIENT_ID_CONFIG to "payment-validator-1",
            ConsumerConfig.GROUP_ID_CONFIG to "test-topic-group",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9093",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
        val consumerOptions: ReceiverOptions<Long, String> = ReceiverOptions.create<Long, String>(consumerProps)
            .subscription(Collections.singleton("test-topic"))
            .addAssignListener { partitions ->
                log.debug("onPartitionsAssigned {}", partitions)
            }
            .addRevokeListener { partitions ->
                log.debug("onPartitionsRevoked {}", partitions)
            }
        /**
         * We create a receiver for new unconfirmed transactions
         */
        kafkaReceiver = KafkaReceiver.create(consumerOptions)


    }

    private fun processEvent(news: NewsItem) {
        println(news)
    }

    @PostConstruct
    fun startConsuming() {
//        kafkaReceiver.receive().subscribe { r ->
//            System.out.printf("Received message: %s\n", r)
//            r.receiverOffset().acknowledge()
//        }

//        kafkaReceiver.receive()
//            .doOnNext { r ->
//                val event = objectMapper.readValue<NewsItem>(r.value())
//                processEvent(event)
//                r.receiverOffset().acknowledge()
//            }.doOnError { e ->
//                log.error(e.message)
//            }
//
//            .subscribe()

       // Thread.sleep(10000)

    }


    companion object {
        private val log: Logger = LoggerFactory.getLogger(KafkaConsumerService::class.java)
        private val objectMapper: ObjectMapper = jacksonObjectMapper()
    }
}

data class NewsItem(val id:Long, val msg:String)