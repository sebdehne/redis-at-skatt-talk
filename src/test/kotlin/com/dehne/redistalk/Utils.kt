package com.dehne.redistalk

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule

val objectMapper = ObjectMapper()
    .registerModule(KotlinModule.Builder().build())
    .registerModules(JavaTimeModule())