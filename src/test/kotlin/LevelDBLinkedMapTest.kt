import com.walkmind.extensions.collections.LevelDBLinkedMap
import com.walkmind.extensions.marshallers.DefaultLongMarshaller
import com.walkmind.extensions.marshallers.DefaultStringMarshaller
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class LevelDBLinkedMapTest {
    @Test
    fun iterateToFirstMatchingPrefix() {
        val path = createTempDir("LEVEL_DB_")
        println(path.absolutePath)
        val instance = LevelDBLinkedMap(path, null, DefaultStringMarshaller, DefaultLongMarshaller)
        instance.use { map ->

            map.put("alpha", 1L)
            map.put("cat", 5L)
            map.put("a", 2L)
            map.put("b", 3L)
            map.put("dome", 6L)

            map.iterator("c").use {
                Assertions.assertTrue(it.hasNext())
                val (key, value) = it.next()
                Assertions.assertEquals(key, "cat")
            }
        }
        instance.destroy()
    }
}