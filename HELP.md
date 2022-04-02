# Read Me First
## Docker
Start containers with:
- `DOCKERHOST=$(ifconfig | grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | grep -v 127.0.0.1 | awk '{ print $2 }' | cut -f2 -d: | head -n1) docker-compose up -d`
- `set-env.sh` also sets the `DOCKERHOST` variable, which is the current IP needed for `KAFKA_ADVERTISED_HOST_NAME`

Go into container:
- `docker-compose exec kafka /bin/bash`

Stop Kafka cleanly
- ` docker-compose down --remove-orphans`

## Commands
- Show Topic metadata: `kcat -b localhost:9093 -L -t test-topic`
- Generate message: `gen.sh`
- Generate single message: `echo '{"id":997,"msg":"Messsage_997"}' | kcat -b localhost:9093 -P -t test-topic -k 997`
- Consume: ` kcat -b localhost:9093 -C \
  -f '\nKey (%K bytes): %k\t\nValue (%S bytes): %s\n\Partition: %p\tOffset: %o\n--\n' \
  -t test-topic`

## SSE Endpoints
- Open SSE: `curl http://localhost:8080/news/sse`
- Open SSE with group: `curl http://localhost:8080/news/sse?group=my-group`


## Other
- https://github.com/edenhill/kcat
- https://jaceklaskowski.gitbooks.io/apache-kafka/content/kafka-docker.html
- https://github.com/conduktor/kafka-stack-docker-compose
- https://projectreactor.io/docs/kafka/release/reference/
- https://github.com/vanseverk/paymentprocessor-kafka-intro
- https://cdmana.com/2021/05/20210504123636842d.html
- https://github.com/reactor/reactor-kafka/blob/main/reactor-kafka-samples/src/main/java/reactor/kafka/samples/SampleProducer.java
