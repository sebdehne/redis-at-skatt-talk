package com.dehne.redistalk.lock

import com.dehne.redistalk.objectMapper
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis

class MultipleReentrantTryRunLockedServiceTest {

    @Test
    fun test() {
        val lockService = MultipleReentrantLockService(objectMapper, Jedis())

        lockService.tryRunLocked(setOf("lock1", "lock2")) {
            lockService.tryRunLocked(setOf("lock2", "lock3")) {
                println("Hello world!")
            }
        }
    }
}