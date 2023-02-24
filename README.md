# Guardian MDS MME Translator

## Setup
- Create `/lib` and copy all jars from nasdaq package (copy all the files from the inner folders to /lib )
  and delete `slf4j-log4j12.jar`, and `log4j.jar`(we will use log4j2 defined in Gradle)
- When developing in your own machine, add to `c:\Windows\System32\Drivers\etc\hosts` the Kafka host and its alias. E.g.
```
172.21.252.175	gqkafkauat2
```
- Build as jar
```
sbt assembly
```

## Config
- `channel`: `eq` (equity) or `fu` (future). This is related to table names.
- `topic`: Kafka topic the instance listens to.
- `db-type`: `redis` or `mysql`.
- `group-id-suffix-override`: suffix on the group id. This is used only in testing. 

If group id is the same the instance will cooperate with each other to consume the data.  

## Test
- Run docker container: redis at localhost:6039 and mysql at localhost:3306