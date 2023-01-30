FROM openjdk:19
LABEL maintainer="Redbull_Group"
ADD target/kafkaToKafka-0.0.1-SNAPSHOT.jar kafka-to-kafka
ENTRYPOINT ["java","-jar","kafka-to-kafka"]