import com.walkmind.extensions.collections.InMemoryLinkedMap
import com.walkmind.extensions.collections.LevelDBLinkedMap
import com.walkmind.extensions.collections.LinkedMap
import com.walkmind.extensions.collections.RocksDBLinkedMap
import com.walkmind.extensions.serializers.ByteBufSerializer
import com.walkmind.extensions.serializers.DefaultLongSerializer
import com.walkmind.extensions.serializers.DefaultStringSerializer
import com.walkmind.extensions.misc.compareBytes
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.lang.RuntimeException

class LinkedMapTest {

    private val testData = mapOf(
            "alf" to 1L,
            "cat" to 5L,
            "ann" to 2L,
            "bot" to 3L,
            "dom" to 6L
    )

    private fun createAndUseDb(clazz: Class<*>, block: (LinkedMap<String, Long>) -> Unit) {
        val path = createTempDir("DB_${clazz.simpleName.toUpperCase()}_")
        println(path.absolutePath)
        val instance =
                when (clazz) {
                    RocksDBLinkedMap::class.java -> RocksDBLinkedMap(path, null, true, ByteBufSerializer.utf8Sized, ByteBufSerializer.long64)
                    LevelDBLinkedMap::class.java -> LevelDBLinkedMap(path, null, DefaultStringSerializer, DefaultLongSerializer)
                    InMemoryLinkedMap::class.java -> InMemoryLinkedMap(
                            { value, prefix ->
                                value.startsWith(prefix)
                            },
                            object : Comparator<String> {
                                override fun compare(left: String?, right: String?): Int {
                                    if (left == null)
                                        return -1; else
                                        if (right == null)
                                            return 1; else
                                            return compareBytes(left.toByteArray(), right.toByteArray())
                                }
                            })
                    else -> throw RuntimeException("Unknown class $clazz")
                }

        try {
            instance.use { map ->
                block(map)
            }
        } finally {
            instance.destroy()
        }
    }

    private fun fillMap(map: LinkedMap<String, Long>) {
        testData.forEach { (k, v) -> map.put(k, v) }
    }

    @ParameterizedTest
    @ValueSource(classes = [RocksDBLinkedMap::class, LevelDBLinkedMap::class, InMemoryLinkedMap::class])
    fun iterateToFirstMatchingPrefix(clazz: Class<*>) {
        createAndUseDb(clazz) { map ->
            fillMap(map)

            map.iterator("c").use {
                Assertions.assertTrue(it.hasNext())
                val (key, _) = it.next()
                Assertions.assertEquals(key, "cat")
            }
        }
    }

    @ParameterizedTest
    @ValueSource(classes = [RocksDBLinkedMap::class, LevelDBLinkedMap::class, InMemoryLinkedMap::class])
    fun iterateRemainingItems(clazz: Class<*>) {
        createAndUseDb(clazz) { map ->
            fillMap(map)

            map.iterator("b").use {

                val keys = mutableListOf<String>()
                var isFirst = false
                while (it.hasNext()) {
                    keys.add(it.next().first)
                    if (!isFirst) {
                        isFirst = true
                        map.put("zara", 100)
                    }
                }

                // It must not be tailing iterator
                Assertions.assertEquals(keys, listOf("bot", "cat", "dom"))
            }
        }
    }

    @ParameterizedTest
    @ValueSource(classes = [RocksDBLinkedMap::class, LevelDBLinkedMap::class, InMemoryLinkedMap::class])
    fun checkIsEmpty(clazz: Class<*>) {
        createAndUseDb(clazz) { map ->
            Assertions.assertTrue(map.isEmpty())
            fillMap(map)
            Assertions.assertFalse(map.isEmpty())
        }
    }

    @ParameterizedTest
    @ValueSource(classes = [RocksDBLinkedMap::class, LevelDBLinkedMap::class, InMemoryLinkedMap::class])
    fun checkGet(clazz: Class<*>) {
        createAndUseDb(clazz) { map ->
            fillMap(map)
            Assertions.assertNull(map.get("MISSING"))
            testData.forEach { (k, v) -> Assertions.assertEquals(map.get(k), v) }
        }
    }

    @ParameterizedTest
    @ValueSource(classes = [RocksDBLinkedMap::class, LevelDBLinkedMap::class, InMemoryLinkedMap::class])
    fun checkClear(clazz: Class<*>) {
        createAndUseDb(clazz) { map ->
            fillMap(map)
            Assertions.assertNotNull(map.get(testData.keys.first()))
            Assertions.assertFalse(map.isEmpty())
            map.clear()

            testData.keys.forEach { k -> Assertions.assertNull(map.get(k)) }

            Assertions.assertTrue(map.isEmpty())
        }
    }

    @ParameterizedTest
    @ValueSource(classes = [RocksDBLinkedMap::class, LevelDBLinkedMap::class, InMemoryLinkedMap::class])
    fun checkOverwrite(clazz: Class<*>) {
        createAndUseDb(clazz) { map ->
            // First pass
            testData.forEach { (k, v) -> map.put(k, -v) }

            // Second pass
            fillMap(map)

            // Values of first pass should be overridden
            testData.forEach { (k, v) -> Assertions.assertEquals(map.get(k), v) }
        }
    }

    @ParameterizedTest
    @ValueSource(classes = [RocksDBLinkedMap::class, LevelDBLinkedMap::class, InMemoryLinkedMap::class])
    fun checkPutAll(clazz: Class<*>) {
        createAndUseDb(clazz) { map ->

            map.putAll(testData.map { e -> e.toPair() })
            testData.forEach { (k, v) -> Assertions.assertEquals(map.get(k), v) }
        }
    }

    @ParameterizedTest
    @ValueSource(classes = [RocksDBLinkedMap::class, LevelDBLinkedMap::class, InMemoryLinkedMap::class])
    fun checkSingleDelete(clazz: Class<*>) {
        createAndUseDb(clazz) { map ->

            fillMap(map)
            val key = testData.keys.random()
            map.remove(key)
            Assertions.assertNull(map.get(key))
            map.remove("MISSING")
            Assertions.assertNull(map.get("MISSING"))
        }
    }

    @ParameterizedTest
    @ValueSource(classes = [RocksDBLinkedMap::class, LevelDBLinkedMap::class, InMemoryLinkedMap::class])
    fun checkRemoveRange(clazz: Class<*>) {
        createAndUseDb(clazz) { map ->

            fillMap(map)

            val keys = testData.keys.sorted()

            // removeRange() last parameter is EXCLUSIVE
            map.removeRange(keys.first(), keys.last())
            testData
                    .filter { (k, _) -> k != keys.last() }
                    .forEach { (k, _) -> Assertions.assertNull(map.get(k)) }

            Assertions.assertEquals(map.get(keys.last()), testData[keys.last()])
        }
    }

    @ParameterizedTest
    @ValueSource(classes = [RocksDBLinkedMap::class, LevelDBLinkedMap::class, InMemoryLinkedMap::class])
    fun checkFirst(clazz: Class<*>) {
        createAndUseDb(clazz) { map ->
            fillMap(map)
            val keys = testData.keys.sorted()
            Assertions.assertEquals(map.firstKey(), keys.first())
        }
    }

    @ParameterizedTest
    @ValueSource(classes = [RocksDBLinkedMap::class, LevelDBLinkedMap::class, InMemoryLinkedMap::class])
    fun checkLast(clazz: Class<*>) {
        createAndUseDb(clazz) { map ->
            fillMap(map)
            val keys = testData.keys.sorted()
            Assertions.assertEquals(map.lastKey(), keys.last())
        }
    }
}

