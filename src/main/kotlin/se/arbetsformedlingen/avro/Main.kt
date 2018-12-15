package se.arbetsformedlingen.avro

import com.google.gson.JsonParser
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.DefaultHelpFormatter
import com.xenomachina.argparser.mainBody

private class Args(parser: ArgParser) {
    val name by parser.storing(
        "-n", "--name",
        help = "a JSON string providing the name of the record (required)")

    val doc by parser.storing(
        "-d", "--doc",
        help = "a JSON string providing documentation to the user of this schema (required)")

    val namespace by parser.storing(
        "-n", "--namespace",
        help = "a JSON string that qualifies the name (required)")

}

fun main(args: Array<String>) = mainBody {
    val prologue = """Creates an Avro Schema from a bunch of JSON examples sent into standard in.
        |Send either a single JSON record or multiple JSON records in an JSON array.
        |""".trimMargin()
    ArgParser(args, helpFormatter = DefaultHelpFormatter(prologue = prologue)).parseInto(::Args).run {
        var jsonObject = JsonParser().parse(System.`in`.reader())
        if (jsonObject.isJsonArray) {
            val array = jsonObject.asJsonArray

            val iterable = array.iterator()

            var schemaGenerator = AvroSchemaGenerator(iterable.next(), name, doc, namespace)

            while (iterable.hasNext()) {
                schemaGenerator.addExample(iterable.next())
            }
            println(schemaGenerator.generateSchema().toString(true))
        } else if (jsonObject.isJsonObject) {
            var schemaGenerator =
                AvroSchemaGenerator(jsonObject.asJsonObject, name, doc, namespace)
            println(schemaGenerator.generateSchema().toString(true))
        } else {
            println("Send a single JSON record or several JSON records in a JSON array to standard in")
        }
    }
}