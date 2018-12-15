package se.arbetsformedlingen.avro

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import java.nio.ByteBuffer

object Parse {
    internal fun isMatching(je: JsonElement, type: Schema.Type): Boolean {
        if (je.isJsonNull)
            return type == Schema.Type.NULL

        if (je.isJsonPrimitive) {
            if (je.asJsonPrimitive.isBoolean)
                return type == Schema.Type.BOOLEAN
            if (je.asJsonPrimitive.isString)
                return type == Schema.Type.STRING
            if (je.asJsonPrimitive.isNumber) {
                return when (type) {
                    Schema.Type.DOUBLE -> true
                    Schema.Type.FLOAT -> true
                    Schema.Type.INT -> true
                    Schema.Type.LONG -> true
                    Schema.Type.BYTES -> true
                    Schema.Type.FIXED -> true
                    else -> false
                }
            }
        }

        if (je.isJsonArray) {
            return type == Schema.Type.ARRAY
        }

        if (je.isJsonObject) {
            return type == Schema.Type.RECORD
        }

        return false
    }

    fun avroObjectFromJson(je: JsonElement, schema: Schema): Any? {
        if (je.isJsonNull)
            return null

        if (schema.type == Schema.Type.UNION) {
            val matchingType = schema.types.filter { isMatching(je, it.type) }.firstOrNull()
            if (matchingType == null) {
                return null
            }
            return avroObjectFromJson(je, matchingType)
        }

        if (je.isJsonPrimitive) {
            if (je.asJsonPrimitive.isBoolean)
                return je.asBoolean
            if (je.asJsonPrimitive.isString)
                return je.asString
            if (je.asJsonPrimitive.isNumber) {
                val num = je.asBigDecimal

                return when (schema.type) {
                    Schema.Type.DOUBLE -> num.toDouble()
                    Schema.Type.FLOAT -> num.toFloat()
                    Schema.Type.INT -> num.toInt()
                    Schema.Type.LONG -> num.toLong()
                    Schema.Type.BYTES -> ByteBuffer.wrap(num.setScale((schema.logicalType as LogicalTypes.Decimal).scale).unscaledValue().toByteArray())
                    else -> throw IllegalArgumentException("Expected " + schema.type.getName() + " but got number" + je.toString())
                }
            }
        }

        if (je.isJsonArray) {
            val array = je.asJsonArray.filterNot { it.isJsonObject && it.asJsonObject.keySet().isEmpty() }

            return array.map { avroObjectFromJson(it, schema.elementType) }.toList()
        }

        if (je.isJsonObject) {
            val builder = GenericRecordBuilder(schema)
            je.asJsonObject.entrySet().forEach {
                builder.set(
                    it.key,
                    avroObjectFromJson(
                        it.value,
                        schema.getField(it.key).schema()
                    )
                )
            }
            return builder.build()
        }

        throw IllegalArgumentException("JsonElement type not supported: " + je.asString)
    }

    /**
     * Creates a GenericRecord from [schema] that contains the same info as [je] but with serializable & Avro approved
     * Java objects.
     *
     * Meant to be used with KafkaAvroSerializer.
     */
    fun avroRecordFromJson(je: JsonObject, schema: Schema): GenericRecord {
        val builder = GenericRecordBuilder(schema)
        je.asJsonObject.entrySet().forEach {
            builder.set(
                it.key,
                avroObjectFromJson(
                    it.value,
                    schema.getField(it.key).schema()
                )
            )
        }
        return builder.build()
    }
}