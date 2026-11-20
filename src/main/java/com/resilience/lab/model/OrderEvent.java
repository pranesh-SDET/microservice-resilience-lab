package com.resilience.lab.model;

public record OrderEvent(
        String eventId,
        String customerId,
        long orderId,
        double amount,
        String status
) {}