package com.example.kafka.controller

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap


data class CommitMessage(val consumerKey: String, val toCommit: Map<TopicPartition, OffsetAndMetadata>)

@RestController
class SseController(val adminClient: AdminClient,
                    @Value("\${kafka.boostrap.server}") val boostrapServer:String,
                    @Value("\${server.port}") val serverPort:Int,
) {

    val consumers = ConcurrentHashMap<String, Pair<KafkaConsumer<*, *>, CommitMessage?>>()

    @GetMapping("/infinite/sse", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun sseFlow(): Flow<ServerSentEvent<String>> = flow {
        generateSequence(0) { it + 1 }.forEach {
            emit(
                ServerSentEvent.builder<String>().event("hello-sse-event").id(it.toString())
                    .data("Your lucky number is $it").build()
            )
            delay(500L)
        }
    }

    @PutMapping("/consumers/{consumerKey}/offsets/{offsets}")
    suspend fun offset(@PathVariable("consumerKey") consumerKey: String, @PathVariable("offsets") offsets: String) {
        val toCommit = offsets.toKafkaOffsetMap().map { (partition, offset) ->
            TopicPartition(TEST_TOPIC, partition) to OffsetAndMetadata(offset)
        }.toMap().let{CommitMessage(consumerKey, it)}

        //Commit via admin API only works when NOT consumer is connected
        //val result = adminClient.alterConsumerGroupOffsets(group, toCommit)
        //log.info(result.all().get().toString())

        consumers[consumerKey]?.let { (consumer, _) ->
            consumers[consumerKey] = consumer to toCommit
            //won't work because all operations for consumer need to happen in the dedicated Consumer Thread
            //consumer.commitAsync(toCommit.toMutableMap(), (OffsetCommitCallback {particionts, ex -> log.info(particionts.toString(), ex)}))
        } ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Consumer with id=[$consumerKey] to found")
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @GetMapping("/events/sse", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    suspend fun offsetSSEFlowWithHeartbeat(
        @RequestParam("group", defaultValue = "news-group-1") group: String = "news-group-1",
        @RequestParam("offsets") offsets: String? = null,
        response: ServerHttpResponse,
    ): Flow<ServerSentEvent<String>> = coroutineScope {
        response.headers.apply {
            set("X-Accel-Buffering", "no")
            set("Cache-Control", "no-cache")
        }
        receiver(
            group = group,
            partitionOffsets = offsets?.toKafkaOffsetMap() ?: mutableMapOf()
        ).let { (receiver, consumerKey) ->
            flow {
                //this initial event is needed to re-route ack traffic to the container, which started the SSE, since
                //acks can only be executed by the consumer that served the original message
                emit(consumerInfoEvent(consumerKey, "session-${uuid()}"))
                //start consumer
                receiver.receive().doOnSubscribe {
                    receiver.doOnConsumer{ consumer -> consumer to consumer.partitionsFor(TEST_TOPIC) }
                        .doOnSuccess{ (consumer, partitions) ->
                            log.info("Partitions for ${consumer.groupMetadata()}: $partitions")
                            val underlying = underlyingKafkaConsumer(consumer)
                            consumers[consumerKey] = underlying to null
                            log.info("Added consumer with key=[$consumerKey]")
                        }.subscribe()
                }.asFlow().collect { rec ->
                    // receiver.receive().asFlow().collect { rec ->
                    "key=${rec.key()} offset=${rec.offset()} partition=${rec.partition()} topic=${rec.topic()}".also(log::info)
                    val offset = "${rec.partition()}:${rec.offset()}"
                    //here we commit the previously received offset, which MUST happen here, because the
                    //KafkaConsumer must be addressed within it's dedicated Thread. The dedicated Thread
                    //is only available here
                    consumers[consumerKey]?.let{(consumer, commitMessage) ->
                        commitMessage?.let{consumer.commitSync(commitMessage.toCommit)}
                        log.info("Committed $commitMessage")
                    }

                    emit(
                        ServerSentEvent.builder<String>().event("news-event").id(offset).data(rec.value())
                            .build()
                    )//.also{rec.receiverOffset().acknowledge()}
                }
                emit(null)
            }.transformLatest {
                if (it != null) emit(it)
                while (true) {
                    delay(20_000)
                    emit(HEART_BEAT_SERVER_SENT_EVENT)
                }
            }.onCompletion {
                consumers.remove(consumerKey)
                log.info("Removed consumer with key=[$consumerKey]")
            }
        }
    }


    suspend fun receiver(
        group: String = uuid(),
        consumerId: String = uuid(),
        topic: String = TEST_TOPIC,
        partitionOffsets: MutableMap<Int, Long> = mutableMapOf()
    ): Pair<KafkaReceiver<String, String>, String> {
        val consumerProps = mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            //ConsumerConfig.CLIENT_ID_CONFIG to "fixed-client",// UUID.randomUUID().toString(),//"news-client-1",
            ConsumerConfig.GROUP_ID_CONFIG to group,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to boostrapServer,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
        val consumerOptions: ReceiverOptions<String, String> = ReceiverOptions.create<String, String>(consumerProps)
            .subscription(Collections.singleton(topic))
            .addAssignListener { partitions ->
                log.info("onPartitionsAssigned {}", partitions)
                partitionOffsets.map { (partition, offset) ->
                    partitions.firstOrNull { it.topicPartition().partition() == partition }?.apply {
                        log.info("start seeking partition $this with offset $offset")
                        this.seek(offset)
                    }
                }
                partitionOffsets.clear()
            }
            .addRevokeListener { partitions ->
                log.info("onPartitionsRevoked {}", partitions)
            }.commitInterval(Duration.ZERO)
            .commitBatchSize(0)

        val consumerKey = "$consumerId-$group"
        return KafkaReceiver.create(consumerOptions) to consumerKey
    }

    private fun consumerInfoEvent(consumerKey: String, sessionId: String) =
        ServerSentEvent.builder<String>().event("consumer-connected")
            .data(objectMapper.writeValueAsString(mapOf("consumerId" to consumerKey, "sessionId" to sessionId)))
            .comment("This metadata is needed to acknowledge offsets via a separate PUT request to: http://localhost:$serverPort/consumers/$consumerKey/offsets/{offsets}. " +
                    "\n- Provide the offsets in the form of: <partition1>:<offset1>,<partition-n>:<offset-n>. E.g. 0:10,1:20,2:23" +
                    "\n- Use a cookie SessionId=[$sessionId] so that the ack request can be routed to the container of the consumer that serves the SSE connection")
            .build()

    companion object {
        private val log: Logger = LoggerFactory.getLogger(SseController::class.java)
        private val objectMapper: ObjectMapper = jacksonObjectMapper()
        val HEART_BEAT_SERVER_SENT_EVENT: ServerSentEvent<String> = ServerSentEvent.builder<String>().data("").build()

        fun String.toKafkaOffsetMap() =
            this.split(",").map { it.split(":").let { (partition, offset) -> partition.toInt() to offset.toLong() } }
                .toMap().toMutableMap()

        fun uuid() = UUID.randomUUID().toString().replace("-", "")

        val TEST_TOPIC = "test-topic"

        /**
         * Heavy hack to retrieve KafkaConsumer from Proxy
         */
        fun underlyingKafkaConsumer(consumer: Consumer<*, *>) =
            java.lang.reflect.Proxy.getInvocationHandler(consumer).let { ih ->
                ih.javaClass.declaredFields.first().run {
                    isAccessible = true
                    get(ih)
                }.let { ch ->
                    ch.javaClass.declaredFields.first { it.name == "consumer" }.run {
                        isAccessible = true
                        get(ch)
                    } as KafkaConsumer<*, *>
                }
            }
    }

}