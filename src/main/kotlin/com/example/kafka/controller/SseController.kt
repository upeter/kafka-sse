package com.example.kafka.controller

import com.example.kafka.service.AdminClientExample
import com.example.kafka.service.KafkaConsumerService
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import org.apache.kafka.clients.CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.ConsumerGroupDescription
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.aop.framework.Advised
import org.springframework.aop.support.AopUtils
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap


@Configuration
class KafkaConfig {
    @Bean
    open fun kafkaAdmin(): KafkaAdmin =
         KafkaAdmin(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9093", SESSION_TIMEOUT_MS_CONFIG to "100000"))

    @Bean
    fun kafkaAdminClient(kafkaAdmin:KafkaAdmin): AdminClient {
        return AdminClient.create(kafkaAdmin.configurationProperties)
    }

}

data class CommitMessage(val consumerKey: String, val toCommit: Map<TopicPartition,OffsetAndMetadata>)
@RestController
class SseController(val adminClient: AdminClient) {

    val consumers = ConcurrentHashMap<String, KafkaConsumer<*, *>>()


    val channel = BroadcastChannel<CommitMessage>(Channel.BUFFERED)

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



//    @PutMapping("/consumers/groups/{group}/offsets/{offsets}")
//    fun offset(  @PathVariable("group") group: String = "news-group-1", @PathVariable("offsets") offsets: String) {
        @PutMapping("/consumers/{consumerKey}/offsets/{offsets}")
        suspend fun offset(  @PathVariable("consumerKey") consumerKey: String, @PathVariable("offsets") offsets: String) {
//        // Describe a group
//        val groupDescription: ConsumerGroupDescription = adminClient
//            .describeConsumerGroups(listOf("a"))
//            .describedGroups().get("a")?.get()!!
//        println("Description of group " + "a" + ":" + groupDescription)
//
//        val toCommit = offsets.toKafkaOffsetMap().map { (partition, offset) ->
//            TopicPartition(TEST_TOPIC, partition) to OffsetAndMetadata(offset)
//        }.toMap()
//        val result = adminClient.alterConsumerGroupOffsets(group, toCommit)
//        log.info(result.all().get().toString())
//        adminClient.alterConsumerGroupOffsets()


    consumers[consumerKey]?.let { consumer ->
            val toCommit = offsets.toKafkaOffsetMap().map { (partition, offset) ->
                TopicPartition(TEST_TOPIC, partition) to OffsetAndMetadata(offset)
            }.toMap()
        channel.send(CommitMessage(consumerKey, toCommit))
        //consumer.commitAsync(toCommit.toMutableMap(), (OffsetCommitCallback {particionts, ex -> log.info(particionts.toString(), ex)}))
        } ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Consumer with id=[$consumerKey] to found")
    }

    @GetMapping("/hi", produces = [MediaType.TEXT_PLAIN_VALUE])
    fun sampleGet(response: ServerHttpResponse): String =  response.headers.apply {
        set("X-Accel-Buffering", "no")
        set("Cache-Control", "no-cache")
    }.let{"Hi"}


    @OptIn(ExperimentalCoroutinesApi::class)
    @GetMapping("/events/sse-simple", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    suspend fun offsetSSEFlowWithHeartbeatSimple(
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
                emit(consumerInfoEvent(consumerKey, "session-${uuid()}"))
                receiver.receive().doOnSubscribe {

                    receiver.doOnConsumer({ consumer -> consumer to consumer.partitionsFor(TEST_TOPIC) })
                        .doOnSuccess({ (consumer, partitions) ->

                            println("======Partitions $partitions")
//                            consumer as reactor.kafka.receiver.internals.ConsumerHandler<*, *>
                            val underlying = underlyingKafkaConsumer(consumer)

                            consumers[consumerKey] = underlying
                            log.info("Added consumer with key=[$consumerKey]")
                        }).subscribe()
                }.asFlow().collect { rec ->
                    // receiver.receive().asFlow().collect { rec ->
                    "key=${rec.key()} offset=${rec.offset()} partition=${rec.partition()} topic=${rec.topic()}".also(log::info)
                    val offset = "${rec.partition()}:${rec.offset()}"
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




    @OptIn(ExperimentalCoroutinesApi::class)
    @GetMapping("/events/sse", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    suspend fun offsetSSEFlowWithHeartbeat(
        @RequestParam("group", defaultValue = "news-group-1") group: String = "news-group-1",
        @RequestParam("offsets") offsets: String? = null,
        response: ServerHttpResponse,
    ): Flow<ServerSentEvent<String>> = coroutineScope{
        response.headers.apply {
            set("X-Accel-Buffering", "no")
            set("Cache-Control", "no-cache")
        }
        receiver(
            group = group,
            partitionOffsets = offsets?.toKafkaOffsetMap() ?: mutableMapOf()
        ).let { (receiver, consumerKey) ->



            flow {
                emit(consumerInfoEvent(consumerKey, "session-${uuid()}"))
                flowOf(receiver.receive().doOnSubscribe {

                    receiver.doOnConsumer({ consumer -> consumer to consumer.partitionsFor(TEST_TOPIC) })
                        .doOnSuccess({ (consumer, partitions) ->

                            println("======Partitions $partitions")
//                            consumer as reactor.kafka.receiver.internals.ConsumerHandler<*, *>
                            val underlying = underlyingKafkaConsumer(consumer)
                            log.info("Consumer Thread is: ${Thread.currentThread().name}")
                            consumers[consumerKey] = underlying
                            log.info("Added consumer with key=[$consumerKey]")
                        }).subscribe()
                }.asFlow(), channel.openSubscription().consumeAsFlow()).flattenMerge().collect { rec ->
                    when(rec) {
                        is CommitMessage -> {
                            consumers.get(consumerKey)?.let {
                                log.info("Committing=[$rec]")
                                it.commitSync(rec.toCommit)
                            }
                        }
                        is ReceiverRecord<*, *> -> {
                            // receiver.receive().asFlow().collect { rec ->
                            "key=${rec.key()} offset=${rec.offset()} partition=${rec.partition()} topic=${rec.topic()}".also(
                                log::info
                            )
                            val offset = "${rec.partition()}:${rec.offset()}"
                            emit(
                                ServerSentEvent.builder<String>().event("news-event").id(offset).data(rec.value().toString())
                                    .build()

                            )//.also{rec.receiverOffset().acknowledge()}
                            emit(null)
                        }
                        else -> throw IllegalArgumentException("Unknown type $rec")
                    }
                }
            }.transformLatest {
                //transformLatest runs the lambda in its own coroutine
                //the coroutine is cancelled when a new value arrives
                //delay will throw cancellation exception exiting the while loop below
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
        consumerId:String = uuid(),
        topic: String = TEST_TOPIC,
        partitionOffsets: MutableMap<Int, Long> = mutableMapOf()
    ): Pair<KafkaReceiver<String, String>, String> {
        val consumerProps = mapOf(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            //ConsumerConfig.CLIENT_ID_CONFIG to "fixed-client",// UUID.randomUUID().toString(),//"news-client-1",
            ConsumerConfig.GROUP_ID_CONFIG to group,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9093",
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

//                .doOnConsumer { consumer ->
//                consumer.seek(TopicPartition(), OffsetAndMetadata())
        //}


    }

    private fun consumerInfoEvent(consumerKey:String, sessionId:String) =
        ServerSentEvent.builder<String>().event("consumer-data").data(objectMapper.writeValueAsString(mapOf("consumerId" to consumerKey, "sessionId" to sessionId)))
            .build()

    companion object {
        private val log: Logger = LoggerFactory.getLogger(KafkaConsumerService::class.java)
        private val objectMapper: ObjectMapper = jacksonObjectMapper()
        val HEART_BEAT_SERVER_SENT_EVENT: ServerSentEvent<String> = ServerSentEvent.builder<String>().data("").build()

        fun String.toKafkaOffsetMap() =
            this.split(",").map { it.split(":").let { (partition, offset) -> partition.toInt() to offset.toLong() } }
                .toMap().toMutableMap()

        fun uuid() = UUID.randomUUID().toString().replace("-", "")

        val TEST_TOPIC = "test-topic"

        fun underlyingKafkaConsumer(consumer: Consumer<*, *>)= java.lang.reflect.Proxy.getInvocationHandler(consumer).let { ih ->
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