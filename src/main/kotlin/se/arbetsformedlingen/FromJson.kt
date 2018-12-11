package se.arbetsformedlingen

import com.google.gson.JsonElement
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecordBuilder

object FromJson {
    private fun isMatching(je: JsonElement, type: Schema.Type): Boolean {
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

    fun parseJsonToObject(je: JsonElement, ident: String, schema: Schema): Any? {
        if (je.isJsonNull)
            return null

        if (schema.type == Schema.Type.UNION) {
            val matchingType = schema.types.filter { isMatching(je, it.type) }.firstOrNull()
            if (matchingType == null)
                return null
            return parseJsonToObject(je, ident, matchingType)
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
                    Schema.Type.BYTES -> num.setScale((schema.logicalType as LogicalTypes.Decimal).scale).unscaledValue().toByteArray()
                    else -> throw IllegalArgumentException("Expected " + schema.type.getName() + " but got number" + je.toString())
                }
            }
        }

        if (je.isJsonArray) {
            val array = je.asJsonArray.filterNot { it.isJsonObject && it.asJsonObject.keySet().isEmpty() }

            return array.map { parseJsonToObject(it, ident, schema.elementType) }.toList()
        }

        if (je.isJsonObject) {
            val builder = GenericRecordBuilder(schema)
            je.asJsonObject.entrySet().forEach { builder.set(it.key, parseJsonToObject(it.value, it.key, schema.getField(it.key).schema())) }
            return builder.build()
        }

        throw IllegalArgumentException("JsonElement type not supported: " + je.asString)
    }
}