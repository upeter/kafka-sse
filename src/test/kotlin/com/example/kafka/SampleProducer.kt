package com.example.kafka

import com.example.kafka.SampleProducer.Companion.BOOTSTRAP_SERVERS
import com.example.kafka.SampleProducer.Companion.TOPIC
import com.example.kafka.domain.NewsItem
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.asFlux
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.CountDownLatch

/**
 * Sample producer application using Reactive API for Kafka.
 * To run sample producer
 *
 *  1.  Start Zookeeper and Kafka server
 *  1.  Update [.BOOTSTRAP_SERVERS] and [.TOPIC] if required
 *  1.  Create Kafka topic [.TOPIC]
 *  1.  Run [SampleProducer] as Java application with all dependent jars in the CLASSPATH (eg. from IDE).
 *  1.  Shutdown Kafka server and Zookeeper when no longer required
 *
 */
class SampleProducer(bootstrapServers: String) {
    private val sender: KafkaSender<String, String>
    private val dateFormat: SimpleDateFormat

    init {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ProducerConfig.CLIENT_ID_CONFIG] = "sample-producer"
        props[ProducerConfig.ACKS_CONFIG] = "all"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        val senderOptions = SenderOptions.create<String, String>(props)
        sender = KafkaSender.create(senderOptions)
        dateFormat = SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy")
    }

    suspend fun sendMessages2(topic: String, count: Int): Flow<SenderResult<Int>> {
        val flow = flow<SenderRecord<String, String, Int>> {
            (1..count).map {
                emit(
                    SenderRecord.create(
                        ProducerRecord(topic,  it.toString(), mapper.writeValueAsString(NewsItem(it.toLong(), "Messsage_$it"))),
                        it
                    )
                )
                delay(500)
            }.onEach { log.info("Putting {}", it) }
        }
        return flow {
            sender.send(flow.asFlux()).asFlow().collect { r: SenderResult<Int> ->
                emit(r)
                val metadata = r.recordMetadata()
                Companion.log.info(
                    "Message {} sent successfully, topic-partition={}-{} offset={} timestamp={}\n",
                    r.correlationMetadata(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    dateFormat.format(Date(metadata.timestamp()))
                )
            }
        }
    }

    fun sendMessages(topic: String, count: Int, latch: CountDownLatch) {
        val flux: Flux<SenderRecord<String, String, Int>> = Flux.range(1, count)
            .map { i: Int ->
                SenderRecord.create(
                    ProducerRecord(topic, i.toString(), mapper.writeValueAsString(NewsItem(i.toLong(), "Messsage_$i"))),
                    i
                )
            }
        sender.send(flux)
            .doOnError { e: Throwable? ->
                log.error("Send failed", e)
            }
            .subscribe { r: SenderResult<Int> ->
                val metadata = r.recordMetadata()
                log.info(
                    "Message {} sent successfully, topic-partition={}-{} offset={} timestamp={}\n",
                    r.correlationMetadata(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    dateFormat.format(Date(metadata.timestamp()))
                )
                latch.countDown()
            }
    }

    fun close() {
        sender.close()
    }

    companion object {
        private val log = LoggerFactory.getLogger(SampleProducer::class.java.name)
        val mapper = jacksonObjectMapper()
        const val BOOTSTRAP_SERVERS = "localhost:9093"
        const val TOPIC = "test-topic"

    }
}

suspend fun main(args: Array<String>):Unit = coroutineScope {
    val count = 1000
    val producer = SampleProducer(BOOTSTRAP_SERVERS)
        producer.sendMessages2(TOPIC, count).onCompletion { producer.close() }.collect()

}