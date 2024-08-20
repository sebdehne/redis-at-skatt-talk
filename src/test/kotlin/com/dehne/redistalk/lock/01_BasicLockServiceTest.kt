package com.dehne.redistalk.lock

import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis

class BasicTryRunLockedServiceTest {

    @Test
    fun test() {
        val basicLockService = BasicLockService(Jedis())

        basicLockService.tryRunLocked("test") {

        }
    }
}