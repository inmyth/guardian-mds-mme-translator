# Guardian MDS MME Translator

## Setup

- When developing in your own machine, add to `c:\Windows\System32\Drivers\etc\hosts` the Kafka host and its alias. E.g.
```
172.21.252.175	gqkafkauat2
```
- Build as jar
```
sbt assembly
```

## Config
- `topic`: Kafka topic the instance listens to.
- `channel`: `eq` (equity) or `fu` (future). This is related to table names.
- `db-type`: `redis` or `mysql`.
- `kafka-group-id`: Kafka consumer group id. Each database needs a unique id. 
If group id is the same the instance will cooperate with each other to consume the data.  