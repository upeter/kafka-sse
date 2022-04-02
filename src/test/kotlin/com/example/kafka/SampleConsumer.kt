package com.example.kafka

import com.example.kafka.service.KafkaConsumerService
import com.example.kafka.service.NewsItem
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class SampleConsumer( val bootstrapServers: String, val topic:String) {
    private val kafkaReceiver: KafkaReceiver<Long, String>

    init {


    val consumerProps = mapOf(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to IntegerDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.CLIENT_ID_CONFIG to "news-client -1",
        ConsumerConfig.GROUP_ID_CONFIG to "test-topic-1",
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        //ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
    )
    val consumerOptions: ReceiverOptions<Long, String> = ReceiverOptions.create<Long, String>(consumerProps)
        .subscription(Collections.singleton(topic))
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


    companion object {
        private val log = LoggerFactory.getLogger(SampleConsumer::class.java.name)
        val mapper = jacksonObjectMapper()
        private const val BOOTSTRAP_SERVERS = "localhost:9093"
        private const val TOPIC = "test-topic"
        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val count = 20
            val latch = CountDownLatch(count)
            val consumer = SampleConsumer(BOOTSTRAP_SERVERS, TOPIC)
            consumer.kafkaReceiver.receive().doOnError{ e: Throwable ->

                System.out.printf("ERROR: %s\n", e)
            }.subscribe { r: ReceiverRecord<Long, String> ->
                System.out.printf("Received message: %s\n", r)
                r.receiverOffset().acknowledge()
            }
            latch.await(100, TimeUnit.SECONDS)
            //consumer.kafkaReceiver.stop
        }
    }
}