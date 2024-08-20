package com.dehne.redistalk.lock

import com.dehne.redistalk.objectMapper
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis
import java.lang.Thread.sleep
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class ImprovedTryRunLockedServiceTest {

    @Test
    fun test() {
        val executorService = Executors.newCachedThreadPool()
        val improvedLockService = ImprovedLockService(objectMapper, Jedis())

        val cnt = CountDownLatch(1)

        executorService.submit {
            improvedLockService.tryRunLocked("test") {
                cnt.countDown()
                sleep(1000)
            }
        }

        cnt.await()
        val secondTx = improvedLockService.tryRunLocked("test") {
            error("")
        }
        check(!secondTx)

    }
}