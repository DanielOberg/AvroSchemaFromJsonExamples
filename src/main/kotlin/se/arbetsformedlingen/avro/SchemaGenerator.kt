package se.arbetsformedlingen.avro

import com.google.gson.JsonElement
import io.confluent.connect.avro.AvroData
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.SchemaAndValue
import java.math.BigDecimal
import java.math.MathContext
import java.text.Normalizer
import java.util.*


fun Schema.toConnectSchemaAndValue(data: GenericRecord): SchemaAndValue {
    val avroData = AvroData(1000)
    return avroData.toConnectData(this, data)
}

fun normalizeAvroFieldName(fieldName: kotlin.String): kotlin.String {
    var result = fieldName.replace('-', '_')
    result = Normalizer.normalize(result, Normalizer.Form.NFD)
    result = result.replace(Regex("[^A-Za-z0-9_]"), "")
    return result
}

/**
 * AvroSchemaGenerator takes in multiple JsonElements and creates an Avro Schema that
 * can handle all cases given.
 *
 * @param rootNodeJson First JSON example e.g. `JsonParser().parse("{'test':5}")`. Use addExample to add more.
 * @param rootName a JSON string providing the name of the record. Will be put in the root node of the schema.
 * @param rootDoc a JSON string providing documentation to the user of this schema. Will be put in the root node of the schema.
 * @param rootNamespace a JSON string that qualifies the namespace. Will be put in the root node of the schema.
 *
 * @sample se.arbetsformedlingen.avro.ExampleCode.example
 */
class SchemaGenerator(rootNodeJson: JsonElement, private val rootName: kotlin.String, private val rootDoc: kotlin.String, private val rootNamespace: kotlin.String) {
    private var currentType = typeTree(rootNodeJson, rootName)

    fun addExample(rootNodeJson: JsonElement): Unit {
        val mergeWith = typeTree(rootNodeJson, rootName)
        currentType = currentType.widening(mergeWith)
    }

    fun generateSchema(): Schema {
        return currentType.toSchemaForAvro(rootName, rootDoc, rootNamespace)
    }

    private sealed class AvroType {
        data class Null(val count: kotlin.Long) : AvroType()
        data class Boolean(val examples: CircleBuffer<kotlin.Boolean>) : AvroType()
        data class Int(val examples: CircleBuffer<BigDecimal>) : AvroType()
        data class Long(val examples: CircleBuffer<BigDecimal>) : AvroType()
        data class Float(val examples: CircleBuffer<BigDecimal>) : AvroType()
        data class Double(val examples: CircleBuffer<BigDecimal>) : AvroType()
        data class Bytes(val examples: CircleBuffer<ByteArray>) : AvroType()
        data class String(val examples: CircleBuffer<kotlin.String>) : AvroType()
        data class Record(val count: kotlin.Long, val name: kotlin.String, val value: Map<kotlin.String, AvroType>) : AvroType()
        data class Enum(val name: kotlin.String, val value: Set<kotlin.String>, val examples: CircleBuffer<kotlin.String>) : AvroType()
        data class Array(val value: AvroType?) : AvroType()
        data class Union(val value: Collection<AvroType>) : AvroType()
        data class Decimal(val scale: kotlin.Int, val precision: kotlin.Int, val examples: CircleBuffer<BigDecimal>) : AvroType()

        private fun order(): kotlin.Int {
            return when (this) {
                is Null -> 0
                is Boolean -> 1
                is Int -> 2
                is Long -> 3
                is Float -> 4
                is Double -> 5
                is Bytes -> 6
                is String -> 7
                is Record -> 8
                is Enum -> 9
                is Array -> 10
                is Union -> 11
                is Decimal -> 12
            }
        }

        private fun className(): kotlin.String {
            return when (this) {
                is Record -> listOf(this::class.java.simpleName, this.name).joinToString(".")
                is Enum -> listOf(this::class.java.simpleName, this.name).joinToString(".")
                else -> this::class.java.simpleName
            }
        }

        private fun countExamples(): List<Pair<kotlin.Long, kotlin.String>> {
            return when (this) {
                is Null -> listOf(Pair(this.count, "null"))
                is Boolean -> this.examples.sortedEntries()
                is Int -> this.examples.sortedEntries()
                is Long -> this.examples.sortedEntries()
                is Float -> this.examples.sortedEntries()
                is Double -> this.examples.sortedEntries()
                is Bytes -> this.examples.sortedEntries()
                is String -> this.examples.sortedEntries().map { Pair(it.first, "'" + it.second + "'") }
                is Record -> listOf(Pair(this.count, "Record"))
                is Enum -> this.examples.sortedEntries()
                is Array -> this.value?.countExamples() ?: listOf()
                is Union -> this.value.flatMap { it.countExamples() }.sortedByDescending { it.first }
                is Decimal -> this.examples.sortedEntries()
            }
        }

        private fun union(t: AvroType): Union {
            var result = LinkedList<AvroType>()

            if (this is Union) {
                result.addAll(this.value)
            } else {
                result.add(this)
            }

            if (t is Union) {
                result.addAll(t.value)
            } else {
                result.add(t)
            }

            // Merge all records
            val records = result.filter { it is Record }
            if (records.count() >= 2) {
                val merged = records.reduce { a, b -> a.widening(b) }
                result = LinkedList(result.filterNot { it is Record })
                result.add(merged)
            }

            // Merge all enums
            val enums = result.filter { it is Enum }
            if (enums.count() >= 2) {
                val merged = enums.reduce { a, b -> a.widening(b) }
                result = LinkedList(result.filterNot { it is Enum })
                result.add(merged)
            }

            // Merge all arrays
            val arrays = result.filter { it is Array }
            if (arrays.count() >= 2) {
                val merged = arrays.reduce { a, b -> a.widening(b) }
                result = LinkedList(result.filterNot { it is Array })
                result.add(merged)
            }


            /*
            Put Null first in the collection

            https://avro.apache.org/docs/1.8.2/spec.html#Unions
            (Note that when a default value is specified for a record field whose
            type is a union, the type of the default value must match the first element
            of the union. Thus, for unions containing "null", the "null" is usually listed
            first, since the default value of such unions is typically null.) */
            val nullValue = result.firstOrNull { it is Null }
            if (nullValue != null) {
                val temp = LinkedList<AvroType>()
                temp.add(nullValue)
                temp.addAll(result.filter { it !is Null })
                result = temp
            }

            val hashMap = result.associate { Pair(it.className(), it) }

            return Union(hashMap.values)
        }


        fun widening(compareTo: AvroType): AvroType {
            val t1 = if (this.order() <= compareTo.order()) this else compareTo
            val t2 = if (this.order() > compareTo.order()) this else compareTo

            when (t1) {
                is Null -> when (t2) {
                    is Null -> return Null(
                        t1.count + t2.count
                    )
                    else -> return t1.union(t2)
                }
                is Boolean -> when (t2) {
                    is Boolean -> return Boolean(
                        t1.examples.merge(t2.examples)
                    )
                    else -> return t1.union(t2)
                }
                is Int -> when (t2) {
                    is Int -> return Int(
                        t1.examples.mergeBigDecimal(t2.examples)
                    )
                    is Long -> return Long(
                        t2.examples.mergeBigDecimal(t1.examples)
                    )
                    is Float -> return Double(
                        t1.examples.mergeBigDecimal(t2.examples)
                    )
                    is Double -> return Double(
                        t1.examples.mergeBigDecimal(t2.examples)
                    )
                    is Decimal -> {
                        val dec1 = BigDecimal(kotlin.Int.MAX_VALUE)
                        return Decimal(
                            maxOf(
                                dec1.scale(),
                                t2.scale
                            ), maxOf(dec1.precision(), t2.precision), t1.examples.mergeBigDecimal(t2.examples)
                        )
                    }
                    else -> return t1.union(t2)
                }
                is Long -> when (t2) {
                    is Long -> return Long(
                        t2.examples.mergeBigDecimal(t1.examples)
                    )
                    is Float -> return Double(
                        t2.examples.mergeBigDecimal(t1.examples)
                    )
                    is Double -> return Double(
                        t2.examples.mergeBigDecimal(t1.examples)
                    )
                    is Decimal -> {
                        val dec1 = BigDecimal(kotlin.Long.MAX_VALUE)
                        return Decimal(
                            maxOf(
                                dec1.scale(),
                                t2.scale
                            ), maxOf(dec1.precision(), t2.precision), t1.examples.mergeBigDecimal(t2.examples)
                        )
                    }
                    else -> return t1.union(t2)
                }
                is Float -> when (t2) {
                    is Float -> return Float(
                        t2.examples.mergeBigDecimal(t1.examples)
                    )
                    is Double -> return Double(
                        t2.examples.mergeBigDecimal(t1.examples)
                    )
                    is Decimal -> {
                        val dec1 = BigDecimal(kotlin.Float.MAX_VALUE.toString(), MathContext.DECIMAL32)
                        return Decimal(
                            maxOf(
                                dec1.scale(),
                                t2.scale
                            ), maxOf(dec1.precision(), t2.precision), t1.examples.mergeBigDecimal(t2.examples)
                        )
                    }
                    else -> return t1.union(t2)
                }
                is Double -> when (t2) {
                    is Double -> return Double(
                        t2.examples.mergeBigDecimal(t1.examples)
                    )
                    is Decimal -> {
                        val dec1 = BigDecimal(kotlin.Float.MAX_VALUE.toString(), MathContext.DECIMAL64)
                        return Decimal(
                            maxOf(
                                dec1.scale(),
                                t2.scale
                            ), maxOf(dec1.precision(), t2.precision), t1.examples.mergeBigDecimal(t2.examples)
                        )
                    }
                    else -> return t1.union(t2)
                }
                is Bytes -> when (t2) {
                    is Bytes -> return Bytes(
                        t1.examples.merge(t2.examples)
                    )
                    else -> return t1.union(t2)
                }
                is String -> when (t2) {
                    is String -> return String(
                        t1.examples.merge(t2.examples)
                    )
                    is Enum -> return String(
                        t1.examples.merge(t2.examples)
                    )
                    else -> return t1.union(t2)
                }
                is Record -> when (t2) {
                    is Record -> {
                        val keyIntersect = t1.value.keys.intersect(t2.value.keys)
                        val t1EntriesDiff = t1.value.entries.filter { !keyIntersect.contains(it.key) }.map { it.toPair().copy(second = it.value.union(
                            Null(1)
                        ))  }
                        val t2EntriesDiff = t2.value.entries.filter { !keyIntersect.contains(it.key) }.map { it.toPair().copy(second = it.value.union(
                            Null(1)
                        ))  }
                        val tEntriesIntersect = keyIntersect.map { key -> Pair(key, t1.value[key]!!.widening(t2.value[key]!!)) }

                        val allEntries = t1EntriesDiff.union(t2EntriesDiff).union(tEntriesIntersect)
                        return Record(
                            t1.count + t2.count,
                            t1.name,
                            allEntries.toMap()
                        )
                    }
                    else -> return t1.union(t2)
                }
                is Enum -> when (t2) {
                    is Enum -> {
                        if (t1.name == t2.name) {
                            return Enum(
                                t1.name,
                                t1.value.union(t2.value),
                                t1.examples.merge(t2.examples)
                            )
                        }
                        return t1.union(t2)
                    }
                    else -> return t1.union(t2)
                }
                is Array -> when (t2) {
                    is Array -> {
                        if (t1.value != null && t2.value != null) {
                            return Array(t1.value.widening(t2.value))
                        } else if (t1.value != null){
                            return Array(t1.value)
                        } else if (t2.value != null){
                            return Array(t2.value)
                        } else {
                            return Array(t2.value)
                        }
                    }
                    else -> return t1.union(t2)
                }
                is Union -> return t1.union(t2)
                is Decimal -> when (t2) {
                    is Decimal -> return Decimal(
                        maxOf(t1.scale, t2.scale),
                        maxOf(t1.precision, t2.precision),
                        t1.examples.merge(t2.examples)
                    )
                    else -> return t1.union(t2)
                }
            }
        }

        fun toSchemaForAvro(name: kotlin.String? = null, doc: kotlin.String? = null, namespace: kotlin.String? = null): Schema {
            val docFn = {a: AvroType ->
                val count = a.countExamples()
                if (count.isEmpty()) {
                    null
                }
                else if (count.count() == 1) {
                    "Always this value: " + count.joinToString { it.second }
                }
                else if (count.count() < CircleBuffer.CircleBufferMaxSize) {
                    check(!count.any{ it.first == 0L})
                    val sum = count.map { it.first }.sum() * 1.0
                    "(" + count.joinToString { "%.1f".format((it.first/sum)*100.0) + "%" } + ") All samples: " + count.joinToString { it.second }
                }
                else {
                    "Most common samples: " + count.joinToString { it.second }
                }
            }

            when (this) {
                is Null -> return Schema.create(Schema.Type.NULL)
                is Boolean -> return Schema.create(Schema.Type.BOOLEAN)
                is Int -> return Schema.create(Schema.Type.INT)
                is Long -> return Schema.create(Schema.Type.LONG)
                is Float -> return Schema.create(Schema.Type.FLOAT)
                is Double -> return Schema.create(Schema.Type.DOUBLE)
                is Bytes -> return Schema.create(Schema.Type.BYTES)
                is String -> return Schema.create(Schema.Type.STRING)
                is Record -> {
                    val fields = this.value.entries.toList().map {
                        val value = it.value
                        var defaultValue = when (value) {
                            is Union -> if (value.value.any { it is Null }) Schema.Field.NULL_VALUE else null
                            else -> null
                        }
                        Schema.Field(it.key, it.value.toSchemaForAvro(it.key, null, "$namespace.$name"), docFn(it.value), defaultValue)
                    }
                    val schema = when (name != null) {
                        true -> Schema.createRecord(name, doc, namespace, false, fields)
                        false -> Schema.createRecord(fields)
                    }

                    return schema
                }
                is Enum -> return Schema.createEnum(this.name, docFn(this), null, this.value.toList())
                is Array -> return Schema.createArray(this.value!!.toSchemaForAvro(name, null, namespace))
                is Union -> {
                    return Schema.createUnion(this.value.toList().map { it.toSchemaForAvro(name, null, namespace) })
                }
                is Decimal -> {
                    var bytes = Schema.create(Schema.Type.BYTES)
                    var dec = LogicalTypes.decimal(this.precision, this.scale)
                    dec.addToSchema(bytes)
                    return bytes
                }
            }
        }

    }


    private fun typeTree(je: JsonElement, ident: String): AvroType {
        if (je.isJsonNull)
            return AvroType.Null(1)

        if (je.isJsonPrimitive) {
            if (je.asJsonPrimitive.isBoolean)
                return AvroType.Boolean(
                    CircleBuffer.from(
                        je.asBoolean
                    )
                )
            if (je.asJsonPrimitive.isString)
                return AvroType.String(
                    CircleBuffer.from(
                        je.asString
                    )
                )
            if (je.asJsonPrimitive.isNumber) {
                val num = je.asBigDecimal

                val isInt = try {
                    num.intValueExact()
                } catch (e: ArithmeticException) {
                    null
                }
                if (isInt != null) return AvroType.Int(
                    CircleBuffer.from(
                        BigDecimal(je.asInt)
                    )
                )

                val isLong = try {
                    num.longValueExact()
                } catch (e: ArithmeticException) {
                    null
                }
                if (isLong != null) return AvroType.Long(
                    CircleBuffer.from(
                        BigDecimal(je.asLong)
                    )
                )

                return AvroType.Decimal(
                    num.scale(),
                    num.precision(),
                    CircleBuffer.from(num)
                )
            }
        }

        if (je.isJsonArray) {
            if (je.asJsonArray.size() == 0) {
                return AvroType.Array(null)
            }
            return AvroType.Array(je.asJsonArray.map { t ->
                typeTree(
                    t,
                    ident
                )
            }.reduce { a, b -> a.widening(b) })
        }

        if (je.isJsonObject) {
            return AvroType.Record(
                1,
                normalizeAvroFieldName(ident),
                je.asJsonObject.entrySet().map { t -> Pair(normalizeAvroFieldName(t.key), typeTree(t.value, t.key)) }.toMap()
            )
        }

        throw IllegalArgumentException("JsonElement type not supported: " + je.asString)
    }

}