package com.dehne.redistalk.lock

import com.dehne.redistalk.groupchat.GroupChatService
import com.dehne.redistalk.objectMapper
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis
import java.time.Duration
import java.util.concurrent.Executors

class LockedServiceTest {

    @Test
    fun demoAutoExtendingExpire() {

        val lockService = LockService(
            objectMapper,
            Jedis(),
            GroupChatService(objectMapper),
            Executors.newCachedThreadPool()
        )

        lockService.tryRunLocked(
            setOf("lock-1"),
            Duration.ofSeconds(30)
        ) { isStillHoldingLock ->

            // some work

            check(isStillHoldingLock())

            // some more work

        }
    }

}