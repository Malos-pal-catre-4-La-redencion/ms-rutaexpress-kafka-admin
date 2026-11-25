package com.rutaexpress.kafkaadmin.web.dto;

/** Las dos políticas de limpieza que Kafka soporta para log.cleanup.policy. */
public enum CleanupPolicy {
    DELETE("delete"),
    COMPACT("compact");

    private final String valorKafka;

    CleanupPolicy(String valorKafka) {
        this.valorKafka = valorKafka;
    }

    public String valorKafka() {
        return valorKafka;
    }
}
