package com.dehne.redistalk.groupchat

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PostConstruct
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

typealias ChannelId = String
typealias SubscriptionId = String

class GroupChatService(private val objectMapper: ObjectMapper) {

    private val redisChannel: String = this::class.java.name + "-" + "channel1"

    private val subscriptions = mutableMapOf<ChannelId, MutableMap<SubscriptionId, Subscription>>()
    private val lock = ReentrantReadWriteLock()

    @PostConstruct
    fun start() {
        // TODO start listening on the pre-defined redisChannel and distribute messages locally via onMessage()
    }

    fun subscribe(channelIds: Set<ChannelId>): Subscription {
        val subId = UUID.randomUUID().toString()
        val sub = Subscription(onUnsubscribe = {
            runLockedWrite {
                channelIds.forEach { channelId ->
                    val map = subscriptions[channelId]
                    map?.remove(subId)
                    if (map?.isEmpty() == true) {
                        subscriptions.remove(channelId)
                    }
                }
            }
        })

        runLockedWrite {
            channelIds.forEach { channelId ->
                subscriptions.getOrPut(channelId) { mutableMapOf() }[subId] = sub
            }
        }

        return sub
    }

    private fun onMessage(messageWrapper: MessageWrapper) {
        runLockedRead { subscriptions[messageWrapper.channelId] ?: emptyMap() }
            .values
            .forEach {
                it.publish(messageWrapper.msg)
            }
    }

    fun notify(channelIds: Set<ChannelId>, msg: String) {
        channelIds.forEach { channelId ->
//            jedis.publish(
//                redisChannel, objectMapper.writeValueAsString(
//                    MessageWrapper(
//                        channelId,
//                        msg
//                    )
//                )
//            )
        }
    }

    private fun <T> runLockedRead(fn: () -> T): T {
        lock.readLock().lock()
        try {
            return fn()
        } finally {
            lock.readLock().unlock()
        }
    }

    private fun runLockedWrite(fn: () -> Unit) {
        lock.writeLock().lock()
        try {
            fn()
        } finally {
            lock.writeLock().unlock()
        }
    }

    companion object {
        class Subscription(private val onUnsubscribe: () -> Unit) {
            private val messageQueue = LinkedBlockingQueue<String>()

            fun publish(message: String) {
                messageQueue.offer(message)
            }

            fun poll(timeout: Duration): String? = messageQueue.poll(
                timeout.toMillis(),
                TimeUnit.MILLISECONDS
            )

            fun unsubscribe() {
                onUnsubscribe()
            }
        }

        data class MessageWrapper(
            val channelId: String,
            val msg: String
        )
    }


}