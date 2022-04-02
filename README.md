# Kafka SSE with Reactor and Kotlin
## Docker
Start kafka-kraft container (without zookeeper) with:
- ` docker-compose up`

If desired: Start kafka/zookeeper containers with:
- Uncomment this setup in `docker-compose.yaml` and comment the kafka-kraft container. Kafka-Kraft is more hassle free. 
- `start.sh` or:
  - `DOCKERHOST=$(ifconfig | grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | grep -v 127.0.0.1 | awk '{ print $2 }' | cut -f2 -d: | head -n1) docker-compose up -d`
  - `set-env.sh` also sets the `DOCKERHOST` variable, which is the current IP needed for `KAFKA_ADVERTISED_HOST_NAME`

Stop Kafka cleanly
- ` docker-compose down -v --remove-orphans`
- If problems arise after restart remove the `data` folder manually

Go into container:
- `docker-compose exec kafka /bin/bash`

## CLI Commands (Require `kcat` to be available)
- Show Topic metadata: `metadata.sh`
- Send message: `sendMsg.sh`
- Consume all: `consumeAll.sh`
- Consume group: `consumeGroup.sh`
- Generate single message: `echo '{"id":997,"msg":"Messsage_997"}' | kcat -b localhost:9093 -P -t test-topic -k 997`
- Consume: ` kcat -b localhost:9093 -C \
  -f '\nKey (%K bytes): %k\t\nValue (%S bytes): %s\n\Partition: %p\tOffset: %o\n--\n' \
  -t test-topic`

## SSE Endpoints
### SSE Endpoints with client-side offset management:
- Open SSE: `curl http://localhost:8080/events/no-ack/sse`
- Open SSE with group: `curl http://localhost:8080/events/no-ack?group=my-group`
- (Re-)Open SSE with group and offsets: `curl http://localhost:8080/events/no-ack?group=my-group?offsets=<partition>:<offset>,<partition>:<offset>,etc.`


### SSE Endpoints with server-side offset management:
- Open SSE: `curl http://localhost:8080/events/ack/sse`
- Open SSE with group: `curl http://localhost:8080/events/ack?group=my-group`

#### Details about commiting offsets: 
The offsets of the consumed messages need to be committed via the same consumer that serves the SSE. 

This limitation is due to the fact that the AdminAPI only allows out-of-context offset commits when no consumer is connected.

Consequently, a request to commit an offset needs to be routed to the container, where the consumer of the SSE lives, which requires some of stickyness. Stickyness can be accomplished with a session-cookie.

The session-cookie as well as the consumer-id needed for an offset commit are emitted as the first message of an SSE. With this information offsets commits can then take place via a separate HTTP PUT request.

The first message will look as follows:
```
event:consumer-connected
:This metadata is needed to acknowledge offsets via a separate PUT request to: http://localhost:8080/consumers/5e811fa5bf5b491cb03fa75ae43140cc-a/offsets/{offsets}.
:- Provide the offsets in the form of: <partition1>:<offset1>,<partition-n>:<offset-n>. E.g. 0:10,1:20,2:23
:- Use a cookie SessionId=[session-ba435064827a4c1e88fd818374718f91] so that the ack request can be routed to the container of the consumer that serves the SSE connection
data:{"consumerId":"5e811fa5bf5b491cb03fa75ae43140cc-a","sessionId":"session-ba435064827a4c1e88fd818374718f91"}
```
It contains information about the endpoint / session needed to commit offsets for the given stream.

- Commit offset endpoint: ` curl  -X PUT http://localhost:8080/consumers/<consumer-id>/offsets/<offsets>` 

## Other
- https://github.com/edenhill/kcat
- https://jaceklaskowski.gitbooks.io/apache-kafka/content/kafka-docker.html
- https://github.com/conduktor/kafka-stack-docker-compose
- https://projectreactor.io/docs/kafka/release/reference/
- https://github.com/vanseverk/paymentprocessor-kafka-intro
- https://cdmana.com/2021/05/20210504123636842d.html
- https://github.com/reactor/reactor-kafka/blob/main/reactor-kafka-samples/src/main/java/reactor/kafka/samples/SampleProducer.java
