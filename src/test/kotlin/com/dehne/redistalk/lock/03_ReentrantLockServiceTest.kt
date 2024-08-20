package com.dehne.redistalk.lock

import com.dehne.redistalk.objectMapper
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis
import java.lang.Thread.sleep
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class ReentrantTryRunLockedServiceTest {

    @Test
    fun test() {
        val executorService = Executors.newCachedThreadPool()
        val lockService = ReentrantLockService(
            objectMapper,
            Jedis()
        )

        val cnt = CountDownLatch(1)

        executorService.submit {
            lockService.tryRunLocked("test") {
                lockService.tryRunLocked("test") {
                    cnt.countDown()
                    println("HERE")
                    sleep(1000)
                }
            }
        }

        cnt.await()
        val secondTx = lockService.tryRunLocked("test") {
            error("")
        }
        check(!secondTx)
    }
}