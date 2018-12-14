# README

AvroSchemaGenerator takes in multiple JsonElements and creates an Avro Schema that
can handle all cases given.

Uses Gson (gradle: com.google.code.gson:gson:2.3) for the JSON parsing.




``` kotlin
import se.arbetsformedlingen.avro.AvroSchemaGenerator

var jsonObject = JsonParser().parse(System.`in`.reader())
if (jsonObject.isJsonArray) {
    val array = jsonObject.asJsonArray

    val iterable = array.iterator()

    var schemaGenerator = AvroSchemaGenerator(iterable.next(), "SomeSchemaName", "Some docs explaining this schema", "com.mycompany.somenamespace")

    while (iterable.hasNext()) {
        schemaGenerator.addExample(iterable.next())
    }
    println(schemaGenerator.generateSchema().toString(true))
}
```

### Parameters

`rootNodeJson` - First JSON example e.g. `JsonParser().parse("{'test':5}")`. Use addExample to add more.

`rootName` - a JSON string providing the name of the record. Will be put in the root node of the schema.

`rootDoc` - a JSON string providing documentation to the user of this schema. Will be put in the root node of the schema.

`rootNamespace` - a JSON string that qualifies the namespace. Will be put in the root node of the schema.

### Constructors

| &lt;init&gt; | `AvroSchemaGenerator(rootNodeJson: JsonElement, rootName: `[`String`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-string/index.html)`, rootDoc: `[`String`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-string/index.html)`, rootNamespace: `[`String`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-string/index.html)`)`<br>AvroSchemaGenerator takes in multiple JsonElements and creates an Avro Schema that can handle all cases given. |

### Functions

| addExample| `fun addExample(rootNodeJson: JsonElement): `[`Unit`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin/-unit/index.html) |
| generateSchema | `fun generateSchema(): Schema` |

