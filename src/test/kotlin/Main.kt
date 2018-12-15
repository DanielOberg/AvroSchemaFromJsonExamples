package se.arbetsformedlingen.avro

import com.google.gson.JsonParser
import org.apache.avro.generic.GenericRecord
import se.arbetsformedlingen.avro.AvroSchemaGenerator
import se.arbetsformedlingen.avro.Parse
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.junit.jupiter.api.Test
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

class ExampleCode {

    /**
     * Generates a schema from two json examples.
     */
    @Test fun example() {

        // Generate Schema

        val jsonExample1 = JsonParser().parse("{'name1' : 5}")
        val jsonExample2 = JsonParser().parse("{'name2':{'name3': 'my value'}}")

        val avroGenerator = AvroSchemaGenerator(jsonExample1, "MyExample", "Testing my schema", "MyNamespace")

        avroGenerator.addExample(jsonExample2) // add as many examples as you like

        val schema = avroGenerator.generateSchema()

        // Print Schema

        println(schema.toString(true))

        // Parse JSON from Schema

        val example1: GenericRecord =
            Parse.avroRecordFromJson(jsonExample1.asJsonObject, schema)  // if you know it's a JsonObject
        val example2: Any = Parse.avroObjectFromJson(jsonExample1, schema)!!  // if you don't know the Json type

        // Show how to send both examples to Kafka:

        val props = Properties()
        props["bootstrap.servers"] = "kafkadomain.com:9092"
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = KafkaAvroSerializer::class.java
        props["schema.registry.url"] = "https://kafkadomain.com:8081"

        if (false) { // Don't send as the kafka urls are just examples
            val kafkaProducer = KafkaProducer<String, GenericRecord>(props)

            kafkaProducer.send(ProducerRecord("MyTopic", "MyExampleKey1", example1))
            kafkaProducer.send(ProducerRecord("MyTopic", "MyExampleKey2", example2 as GenericRecord))
        }
    }
}