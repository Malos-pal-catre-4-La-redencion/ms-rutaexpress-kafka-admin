package com.rutaexpress.kafkaadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Propiedades propias de este servicio (distintas de spring.kafka.*, que
 * configura el AdminClient). Centraliza los valores por defecto que se usan
 * al crear tópicos cuando el llamante no los especifica explícitamente.
 */
@ConfigurationProperties(prefix = "rutaexpress.kafka-admin")
public record KafkaAdminProperties(
        DefaultsTopico defaultsTopico
) {
    public record DefaultsTopico(
            int particiones,
            short factorReplicacion,
            long retentionMs,
            String cleanupPolicy
    ) {
    }
}
