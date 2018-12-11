package se.arbetsformedlingen

import com.google.gson.JsonElement
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.math.BigDecimal
import java.math.MathContext
import java.util.*

class AvroSchemaGenerator(rootNodeJson: JsonElement, private val rootName: kotlin.String, private val rootDoc: kotlin.String, private val rootNamespace: kotlin.String) {
    private var currentType = typeTree(rootNodeJson, rootName)

    fun addExample(rootNodeJson: JsonElement): Unit {
        val mergeWith = typeTree(rootNodeJson, rootName)
        currentType = currentType.widening(mergeWith)
    }

    fun generateSchema(): Schema {
        return currentType.toSchema(rootName, rootDoc, rootNamespace)
    }

    private sealed class AvroType {
        data class Null(val count: kotlin.Long) : AvroType()
        data class Boolean(val examples: CircleBuffer<kotlin.Boolean>) : AvroType()
        data class Int(val examples: CircleBuffer<BigDecimal>) : AvroType()
        data class Long(val examples: CircleBuffer<BigDecimal>) : AvroType()
        data class Float(val examples: CircleBuffer<BigDecimal>) : AvroType()
        data class Double(val examples: CircleBuffer<BigDecimal>) : AvroType()
        data class Bytes(val examples: CircleBuffer<kotlin.ByteArray>) : AvroType()
        data class String(val examples: CircleBuffer<kotlin.String>) : AvroType()
        data class Record(val name: kotlin.String, val value: Map<kotlin.String, AvroType>) : AvroType()
        data class Enum(val name: kotlin.String, val value: Set<kotlin.String>, val examples: CircleBuffer<kotlin.String>) : AvroType()
        data class Array(val value: AvroType) : AvroType()
        data class Union(val value: Collection<AvroType>) : AvroType()
        data class Decimal(val scale: kotlin.Int, val precision: kotlin.Int, val examples: CircleBuffer<BigDecimal>) : AvroType()

        private fun order(): kotlin.Int {
            return when (this) {
                is AvroType.Null -> 0
                is AvroType.Boolean -> 1
                is AvroType.Int -> 2
                is AvroType.Long -> 3
                is AvroType.Float -> 4
                is AvroType.Double -> 5
                is AvroType.Bytes -> 6
                is AvroType.String -> 7
                is AvroType.Record -> 8
                is AvroType.Enum -> 9
                is AvroType.Array -> 10
                is AvroType.Union -> 11
                is AvroType.Decimal -> 12
            }
        }

        private fun className(): kotlin.String {
            return when (this) {
                is AvroType.Record -> listOf(this::class.java.simpleName, this.name).joinToString(".")
                is AvroType.Enum -> listOf(this::class.java.simpleName, this.name).joinToString(".")
                else -> this::class.java.simpleName
            }
        }

        private fun countExamples(): List<Pair<kotlin.Long, kotlin.String>> {
            return when (this) {
                is AvroType.Null -> listOf(Pair(this.count, "null"))
                is AvroType.Boolean -> this.examples.sortedEntries()
                is AvroType.Int -> this.examples.sortedEntries()
                is AvroType.Long -> this.examples.sortedEntries()
                is AvroType.Float -> this.examples.sortedEntries()
                is AvroType.Double -> this.examples.sortedEntries()
                is AvroType.Bytes -> this.examples.sortedEntries()
                is AvroType.String -> this.examples.sortedEntries().map { Pair(it.first, "'" + it.second + "'") }
                is AvroType.Record -> LinkedList()
                is AvroType.Enum -> this.examples.sortedEntries()
                is AvroType.Array -> this.value.countExamples()
                is AvroType.Union -> this.value.flatMap { it.countExamples() }.sortedByDescending { it.first }
                is AvroType.Decimal -> this.examples.sortedEntries()
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
            val records = result.filter { it is AvroType.Record }
            if (records.count() >= 2) {
                val merged = records.reduce { a, b -> a.widening(b) }
                result = LinkedList(result.filterNot { it is AvroType.Record })
                result.add(merged)
            }

            // Merge all enums
            val enums = result.filter { it is AvroType.Enum }
            if (enums.count() >= 2) {
                val merged = enums.reduce { a, b -> a.widening(b) }
                result = LinkedList(result.filterNot { it is AvroType.Enum })
                result.add(merged)
            }

            // Merge all arrays
            val arrays = result.filter { it is AvroType.Array }
            if (arrays.count() >= 2) {
                val merged = arrays.reduce { a, b -> a.widening(b) }
                result = LinkedList(result.filterNot { it is AvroType.Array })
                result.add(merged)
            }


            /*
            Put Null first in the collection

            https://avro.apache.org/docs/1.8.2/spec.html#Unions
            (Note that when a default value is specified for a record field whose
            type is a union, the type of the default value must match the first element
            of the union. Thus, for unions containing "null", the "null" is usually listed
            first, since the default value of such unions is typically null.) */
            val nullValue = result.firstOrNull { it is AvroType.Null }
            if (nullValue != null) {
                val temp = LinkedList<AvroType>()
                temp.add(nullValue)
                temp.addAll(result.filter { it !is AvroType.Null })
                result = temp
            }

            val hashMap = result.associate { Pair(it.className(), it) }

            return Union(hashMap.values)
        }


        fun widening(compareTo: AvroType): AvroType {
            val t1 = if (this.order() <= compareTo.order()) this else compareTo
            val t2 = if (this.order() > compareTo.order()) this else compareTo

            when (t1) {
                is AvroType.Null -> when (t2) {
                    is AvroType.Null -> return AvroType.Null(t1.count + t2.count)
                    else -> return t1.union(t2)
                }
                is AvroType.Boolean -> when (t2) {
                    is AvroType.Boolean -> return AvroType.Boolean(t1.examples.merge(t2.examples))
                    else -> return t1.union(t2)
                }
                is AvroType.Int -> when (t2) {
                    is AvroType.Int -> return AvroType.Int(t1.examples.mergeBigDecimal(t2.examples))
                    is AvroType.Long -> return AvroType.Long(t2.examples.mergeBigDecimal(t1.examples))
                    is AvroType.Float -> return AvroType.Double(t1.examples.mergeBigDecimal(t2.examples))
                    is AvroType.Double -> return AvroType.Double(t1.examples.mergeBigDecimal(t2.examples))
                    is AvroType.Decimal -> {
                        val dec1 = BigDecimal(kotlin.Int.MAX_VALUE)
                        return AvroType.Decimal(maxOf(dec1.scale(), t2.scale), maxOf(dec1.precision(), t2.precision), t1.examples.mergeBigDecimal(t2.examples))
                    }
                    else -> return t1.union(t2)
                }
                is AvroType.Long -> when (t2) {
                    is AvroType.Long -> return AvroType.Long(t2.examples.mergeBigDecimal(t1.examples))
                    is AvroType.Float -> return AvroType.Double(t2.examples.mergeBigDecimal(t1.examples))
                    is AvroType.Double -> return AvroType.Double(t2.examples.mergeBigDecimal(t1.examples))
                    is AvroType.Decimal -> {
                        val dec1 = BigDecimal(kotlin.Long.MAX_VALUE)
                        return AvroType.Decimal(maxOf(dec1.scale(), t2.scale), maxOf(dec1.precision(), t2.precision), t1.examples.mergeBigDecimal(t2.examples))
                    }
                    else -> return t1.union(t2)
                }
                is AvroType.Float -> when (t2) {
                    is AvroType.Float -> return AvroType.Float(t2.examples.mergeBigDecimal(t1.examples))
                    is AvroType.Double -> return AvroType.Double(t2.examples.mergeBigDecimal(t1.examples))
                    is AvroType.Decimal -> {
                        val dec1 = BigDecimal(kotlin.Float.MAX_VALUE.toString(), MathContext.DECIMAL32)
                        return AvroType.Decimal(maxOf(dec1.scale(), t2.scale), maxOf(dec1.precision(), t2.precision), t1.examples.mergeBigDecimal(t2.examples))
                    }
                    else -> return t1.union(t2)
                }
                is AvroType.Double -> when (t2) {
                    is AvroType.Double -> return AvroType.Double(t2.examples.mergeBigDecimal(t1.examples))
                    is AvroType.Decimal -> {
                        val dec1 = BigDecimal(kotlin.Float.MAX_VALUE.toString(), MathContext.DECIMAL64)
                        return AvroType.Decimal(maxOf(dec1.scale(), t2.scale), maxOf(dec1.precision(), t2.precision), t1.examples.mergeBigDecimal(t2.examples))
                    }
                    else -> return t1.union(t2)
                }
                is AvroType.Bytes -> when (t2) {
                    is AvroType.Bytes -> return AvroType.Bytes(t1.examples.merge(t2.examples))
                    else -> return t1.union(t2)
                }
                is AvroType.String -> when (t2) {
                    is AvroType.String -> return AvroType.String(t1.examples.merge(t2.examples))
                    is AvroType.Enum -> return AvroType.String(t1.examples.merge(t2.examples))
                    else -> return t1.union(t2)
                }
                is AvroType.Record -> when (t2) {
                    is AvroType.Record -> {
                        val keyIntersect = t1.value.keys.intersect(t2.value.keys)
                        val t1EntriesDiff = t1.value.entries.filter { !keyIntersect.contains(it.key) }.map { it.toPair().copy(second = it.value.union(AvroType.Null(1)))  }
                        val t2EntriesDiff = t2.value.entries.filter { !keyIntersect.contains(it.key) }.map { it.toPair().copy(second = it.value.union(AvroType.Null(1)))  }
                        val tEntriesIntersect = keyIntersect.map { key -> Pair(key, t1.value[key]!!.widening(t2.value[key]!!)) }

                        val allEntries = t1EntriesDiff.union(t2EntriesDiff).union(tEntriesIntersect)
                        return AvroType.Record(t1.name, allEntries.toMap())
                    }
                    else -> return t1.union(t2)
                }
                is AvroType.Enum -> when (t2) {
                    is AvroType.Enum -> {
                        if (t1.name == t2.name) {
                            return AvroType.Enum(t1.name, t1.value.union(t2.value), t1.examples.merge(t2.examples))
                        }
                        return t1.union(t2)
                    }
                    else -> return t1.union(t2)
                }
                is AvroType.Array -> when (t2) {
                    is AvroType.Array -> {
                        return AvroType.Array(t1.value.widening(t2.value))
                    }
                    else -> return t1.union(t2)
                }
                is AvroType.Union -> return t1.union(t2)
                is AvroType.Decimal -> when (t2) {
                    is AvroType.Decimal -> return AvroType.Decimal(maxOf(t1.scale, t2.scale), maxOf(t1.precision, t2.precision), t1.examples.merge(t2.examples))
                    else -> return t1.union(t2)
                }
            }
        }

        fun toSchema(name: kotlin.String? = null, doc: kotlin.String? = null, namespace: kotlin.String? = null): Schema {
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
                is AvroType.Null -> return Schema.create(Schema.Type.NULL)
                is AvroType.Boolean -> return Schema.create(Schema.Type.BOOLEAN)
                is AvroType.Int -> return Schema.create(Schema.Type.INT)
                is AvroType.Long -> return Schema.create(Schema.Type.LONG)
                is AvroType.Float -> return Schema.create(Schema.Type.FLOAT)
                is AvroType.Double -> return Schema.create(Schema.Type.DOUBLE)
                is AvroType.Bytes -> return Schema.create(Schema.Type.BYTES)
                is AvroType.String -> return Schema.create(Schema.Type.STRING)
                is AvroType.Record -> {
                    val fields = this.value.entries.toList().map {
                        val value = it.value
                        var defaultValue = when (value) {
                            is AvroType.Union -> if (value.value.any { it is AvroType.Null }) Schema.Field.NULL_VALUE else null
                            else -> null
                        }
                        Schema.Field(it.key, it.value.toSchema(it.key, null, "$namespace.$name"), docFn(it.value), defaultValue)
                    }
                    val schema = when (name != null) {
                        true -> Schema.createRecord(name, doc, namespace, false, fields)
                        false -> Schema.createRecord(fields)
                    }

                    return schema
                }
                is AvroType.Enum -> return Schema.createEnum(this.name, docFn(this), null, this.value.toList())
                is AvroType.Array -> return Schema.createArray(this.value.toSchema(name, null, namespace))
                is AvroType.Union -> {
                    return Schema.createUnion(this.value.toList().map { it.toSchema(name, null, namespace) })
                }
                is AvroType.Decimal -> {
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
                return AvroType.Boolean(CircleBuffer.from(je.asBoolean))
            if (je.asJsonPrimitive.isString)
                return AvroType.String(CircleBuffer.from(je.asString))
            if (je.asJsonPrimitive.isNumber) {
                val num = je.asBigDecimal

                val isInt = try {
                    num.intValueExact()
                } catch (e: ArithmeticException) {
                    null
                }
                if (isInt != null) return AvroType.Int(CircleBuffer.from(BigDecimal(je.asInt)))

                val isLong = try {
                    num.longValueExact()
                } catch (e: ArithmeticException) {
                    null
                }
                if (isLong != null) return AvroType.Long(CircleBuffer.from(BigDecimal(je.asLong)))

                return AvroType.Decimal(num.scale(), num.precision(), CircleBuffer.from(num))
            }
        }

        if (je.isJsonArray) {
            return AvroType.Array(je.asJsonArray.map { t -> typeTree(t, ident) }.reduce { a, b -> a.widening(b) })
        }

        if (je.isJsonObject) {
            return AvroType.Record(ident, je.asJsonObject.entrySet().map { t -> Pair(t.key, typeTree(t.value, t.key)) }.toMap())
        }

        throw IllegalArgumentException("JsonElement type not supported: " + je.asString)
    }

}