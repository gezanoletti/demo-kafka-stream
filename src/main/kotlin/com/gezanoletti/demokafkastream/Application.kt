package com.gezanoletti.demokafkastream

import com.gezanoletti.demokafkaproducer.avro.PersonMessageKey
import com.gezanoletti.demokafkaproducer.avro.PersonMessageValue
import com.gezanoletti.demokafkastream.KafkaConstants.APPLICATION_ID
import com.gezanoletti.demokafkastream.KafkaConstants.KAFKA_BROKERS
import com.gezanoletti.demokafkastream.KafkaConstants.SCHEMA_REGISTRY_URL
import com.gezanoletti.demokafkastream.KafkaConstants.STREAM_SOURCE_TOPIC
import com.gezanoletti.demokafkastream.KafkaConstants.STREAM_SINK_TOPIC
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.StreamsConfig.*
import java.util.*

fun main() {
    val properties = mapOf(
        BOOTSTRAP_SERVERS_CONFIG to KAFKA_BROKERS,
        APPLICATION_ID_CONFIG to APPLICATION_ID,
        DEFAULT_KEY_SERDE_CLASS_CONFIG to SpecificAvroSerde::class.java.name,
        DEFAULT_VALUE_SERDE_CLASS_CONFIG to SpecificAvroSerde::class.java.name,
        SCHEMA_REGISTRY_URL_CONFIG to SCHEMA_REGISTRY_URL
    ).toProperties()

    val streamsBuilder = StreamsBuilder()

    val kStream = streamsBuilder.stream<PersonMessageKey, PersonMessageValue>(STREAM_SOURCE_TOPIC)
    kStream
        .peek { _, value -> println("SOURCE -> $value") }
        .filter { key, _ -> key.getId() % 2 == 0 }
        .mapValues(::applyMapValues)
        .peek { _, value -> println("SINK -> $value") }
        .to(STREAM_SINK_TOPIC)

    val topology = streamsBuilder.build()

    val kafkaStreams = KafkaStreams(topology, properties)
    kafkaStreams.start()

    Runtime.getRuntime().addShutdownHook(Thread(kafkaStreams::close))
}

fun applyMapValues(value: PersonMessageValue): PersonMessageValue {
    value.setName(value.getName().toString().toUpperCase())
    value.setSurname(value.getSurname().toString().toUpperCase())
    return value
}

object KafkaConstants {
    const val KAFKA_BROKERS = "localhost:9092"
    const val APPLICATION_ID = "demo-kafka-streams"
    const val SCHEMA_REGISTRY_URL = "http://localhost:8081"
    const val STREAM_SOURCE_TOPIC = "demo-basic-kafka-partitions-avro"
    const val STREAM_SINK_TOPIC = "demo-basic-kafka-partitions-avro-stream"
}
