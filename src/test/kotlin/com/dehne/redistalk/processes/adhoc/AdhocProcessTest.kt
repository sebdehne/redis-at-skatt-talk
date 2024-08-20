package com.dehne.redistalk.processes.adhoc

import com.dehne.redistalk.objectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class AdhocProcessTest {


    @Test
    fun test() {
        val threadPool = Executors.newCachedThreadPool()

        val p = TestProcess(
            objectMapper,
            { Jedis() },
            threadPool
        )

        (0..11).forEach {
            val status = p.startOrGetStatus("input", it == 0)
            println("$it: ${objectMapper.writeValueAsString(status)}")
            Thread.sleep(500)
        }

    }

    class TestProcess(
        private val objectMapper: ObjectMapper,
        jedisSource: () -> Jedis,
        executorService: ExecutorService
    ) : AdhocProcess<String, String>(objectMapper, jedisSource, executorService) {

        override fun inputArgumentsToStableId(inputArguments: String): String {
            return objectMapper.writeValueAsString(inputArguments).hashCode().toString()
        }

        override fun runAdhocTask(inputArguments: String, updateProgress: (progress: String?) -> Unit): String {

            (1..10).forEach {
                Thread.sleep(500)
                updateProgress("${it * 10}%")
            }

            return "Done"
        }
    }
}