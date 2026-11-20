package com.resilience.lab.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ManagedKafkaClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ManagedKafkaClient.class);
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread shutdownHook;

    protected ManagedKafkaClient() {
        this.shutdownHook = new Thread(() -> {
            if (!closed.get()) {
                try {
                    close();
                } catch (Exception ex) {
                    log.warn("Error during JVM shutdown hook cleanup: {}", ex.getMessage());
                }
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    protected void removeShutdownHookQuietly() {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            // JVM is already shutting down
        }
    }
}