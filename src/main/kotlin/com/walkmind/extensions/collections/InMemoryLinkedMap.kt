package com.walkmind.extensions.collections

import java.io.Closeable
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.BiFunction

class InMemoryLinkedMap<K, V> : LinkedMap<K, V>, Closeable, Destroyable {

    private val map: ConcurrentSkipListMap<K, V>
    private val merger: BiFunction<V, V, V>
    private val prefixMatcher: BiFunction<K, K, Boolean>

    // Key marshaller is required only for prefix search in parametrized iterator()
    constructor(prefixMatcher: BiFunction<K, K, Boolean>) {
        this.map = ConcurrentSkipListMap()
        this.merger = BiFunction { _, replacement -> replacement }
        this.prefixMatcher = prefixMatcher
    }

    // Key marshaller is required only for prefix search in parametrized iterator()
    constructor(prefixMatcher: BiFunction<K, K, Boolean>, parent: Comparator<K>) {
        this.map = ConcurrentSkipListMap(parent)
        this.merger = BiFunction { _, replacement -> replacement }
        this.prefixMatcher = prefixMatcher
    }

    // Key marshaller is required only for prefix search in parametrized iterator()
    constructor(prefixMatcher: BiFunction<K, K, Boolean>, parent: Comparator<K>, merger: BiFunction<V, V, V>) {
        this.map = ConcurrentSkipListMap(parent)
        this.merger = merger
        this.prefixMatcher = prefixMatcher
    }

    override fun put(key: K, value: V) {
        map.put(key, value)
    }

    override fun merge(key: K, value: V) {
        map.merge(key, value, merger)
    }

    override fun remove(key: K) {
        map.remove(key)
    }

    override fun clear() {
        map.clear()
    }

    override fun get(key: K): V? {
        return map.get(key)
    }

    override fun isEmpty(): Boolean {
        return map.isEmpty()
    }

    override fun putAll(from: Iterable<Pair<K, V>>) {
        map.putAll(from)
    }

    override fun mergeAll(from: Iterable<Pair<K, V>>) {
        from.forEach { (key, value) ->
            map.merge(key, value, merger)
        }
    }

    override fun removeRange(keyFrom: K, keyTo: K) {
        val keys = map.navigableKeySet().subSet(keyFrom, true, keyTo, false)
        keys.forEach { key -> map.remove(key) }
    }

    override fun firstKey(): K? {
        return map.firstKey()
    }

    override fun lastKey(): K? {
        return map.lastKey()
    }

    private fun iteratorInternal(start: K?): CloseablePeekingIterator<Pair<K, V>> {

        return object : CloseablePeekingIterator<Pair<K, V>> {
            private val iter: Iterator<MutableMap.MutableEntry<K, V>>
            private var peeked: Pair<K, V>? = null

            init {
                iter = map.subMap(map.firstKey(), true, map.lastKey(), true).iterator()
                if (start != null) {
                    while (iter.hasNext()) {
                        val pair = iter.next().toPair()
                        if (prefixMatcher.apply(pair.first, start)) {
                            peeked = pair
                            break
                        }
                    }
                }
            }

            override fun next(): Pair<K, V> {
                if (peeked != null) {
                    val result = peeked!!
                    peeked = null
                    return result
                } else {
                    return nextReal() ?: throw NoSuchElementException()
                }
            }

            override fun peek(): Pair<K, V> {
                if (peeked == null)
                    peeked = nextReal()

                return peeked ?: throw NoSuchElementException()
            }

            override fun hasNext(): Boolean {
                return iter.hasNext()
            }

            private fun nextReal(): Pair<K, V>? {
                if (iter.hasNext()) {
                    return iter.next().toPair()
                } else
                    return null
            }

            override fun close() {
                peeked = null
            }
        }
    }

    override fun iterator(): CloseablePeekingIterator<Pair<K, V>> {
        return iteratorInternal(null)
    }

    override fun iterator(prefix: K): CloseablePeekingIterator<Pair<K, V>> {
        return iteratorInternal(prefix)
    }

    override fun close() {
    }

    override fun destroy() {
    }
}
