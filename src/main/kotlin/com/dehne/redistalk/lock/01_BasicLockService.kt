package com.dehne.redistalk.lock

import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams
import java.util.*

class BasicLockService(private val jedis: Jedis) {

    fun tryRunLocked(
        lock: String,
        expiresSeconds: Long = 30,
        fn: () -> Unit
    ): Boolean {
        val randomValue = lock(lock, expiresSeconds) ?: return false
        return try {
            fn()
            true
        } finally {
            unlock(lock, randomValue)
        }
    }

    private fun namespaceKey(lock: String) = "${this::class.java.name}-$lock"

    private fun lock(lock: String, expiresSeconds: Long = 30): String? {
        val key = namespaceKey(lock)

        val randomValue = UUID.randomUUID().toString()
        return if (jedis.set(
                key,
                randomValue,
                SetParams()
                    .nx()
                    .ex(expiresSeconds)
            ) == null
        ) {
            null
        } else {
            randomValue
        }
    }

    private fun unlock(lock: String, randomValue: String) {
        val key = namespaceKey(lock)

        jedis.eval(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else 
                return 0
            end
            """.trimIndent(),
            listOf(key),
            listOf(randomValue)
        )
    }
}