package se.arbetsformedlingen.avro

import java.math.BigDecimal
import java.util.*
import kotlin.collections.HashMap

class CircleBuffer<T> {
    private val queue = HashSet<T>()
    private val uniqueCountMap = HashMap<Int, Long>()

    companion object {
        fun <T> from(initValue: T): CircleBuffer<T> {
            val result = CircleBuffer<T>()
            result.push(initValue)
            return result
        }

        val CircleBufferMaxSize: Int = 5
    }

    private fun HashMap<Int, Long>.inc(hash: Int, with: Long = 1L): Long {
        val count = this.getOrDefault(hash, 0L)
        this.set(hash, count + with)
        return this.getOrDefault(hash, 0L)
    }

    fun push(item: T): T {
        queue.add(item)

        val hash = item.hashCode()
        uniqueCountMap.inc(hash)

        if (queue.size > CircleBufferMaxSize) {
            val largest = uniqueCountMap.entries.sortedByDescending { it.value }.map { it.key }.take(CircleBufferMaxSize).toSet()
            queue.removeIf { !largest.contains(it.hashCode()) }
        }

        return item
    }

    fun sortedEntries(): List<Pair<Long, String>> {
        return queue.map { Pair(uniqueCountMap.getOrDefault(it.hashCode(), 0L), it.toString()) }.sortedByDescending { it.first }
    }

    fun merge(withBuffer: CircleBuffer<T>): CircleBuffer<T> {
        val result = CircleBuffer<T>()

        this.iterator().forEach { result.push(it) }
        withBuffer.iterator().forEach { result.push(it) }

        this.uniqueCountMap.entries.forEach { result.uniqueCountMap.inc(it.key, it.value) }
        withBuffer.uniqueCountMap.entries.forEach { result.uniqueCountMap.inc(it.key, it.value) }

        return result
    }

    fun mergeBigDecimal(withBuffer: CircleBuffer<BigDecimal>): CircleBuffer<BigDecimal> {
        val result = CircleBuffer<BigDecimal>()

        this.iterator().forEach { result.push(BigDecimal(it.toString())) }
        withBuffer.iterator().forEach { result.push(BigDecimal(it.toString())) }

        this.uniqueCountMap.entries.forEach { result.uniqueCountMap.inc(it.key, it.value) }
        withBuffer.uniqueCountMap.entries.forEach { result.uniqueCountMap.inc(it.key, it.value) }

        return result
    }

    fun iterator(): MutableIterator<T> {
        return queue.iterator()
    }
}