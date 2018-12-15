# README

AvroSchemaGenerator takes in multiple JsonElements and creates an Avro Schema that
can handle all cases given.

Uses Gson (gradle: com.google.code.gson:gson:2.3) for the JSON parsing.

You can find this Kotlin-package in Maven at [https://bintray.com/arbetsformedlingen](https://bintray.com/arbetsformedlingen/avro/AvroSchemaFromJsonExamples)



``` kotlin
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
```



### Parameters

`rootNodeJson` - First JSON example e.g. `JsonParser().parse("{'test':5}")`. Use addExample to add more.

`rootName` - a JSON string providing the name of the record. Will be put in the root node of the schema.

`rootDoc` - a JSON string providing documentation to the user of this schema. Will be put in the root node of the schema.

`rootNamespace` - a JSON string that qualifies the namespace. Will be put in the root node of the schema.

### Constructors

| Name  | Signature | Comments |
| ------------- | ------------- | ------------- |
| &lt;init&gt; | `AvroSchemaGenerator(rootNodeJson: JsonElement, rootName: `[`String`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-string/index.html)`, rootDoc: `[`String`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-string/index.html)`, rootNamespace: `[`String`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-string/index.html)`)` | AvroSchemaGenerator takes in multiple JsonElements and creates an Avro Schema that can handle all cases given. |

### Functions

| Name  | Signature |
| ------------- | ------------- |
| addExample| `fun addExample(rootNodeJson: JsonElement): `[`Unit`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-unit/index.html) |
| generateSchema | `fun generateSchema(): Schema` |

