package com.rutaexpress.kafkaadmin.web.dto;

import jakarta.validation.constraints.Positive;

/**
 * Request de modificación. Kafka no permite reducir particiones ni cambiar
 * el factor de replicación de un tópico existente sin una reasignación de
 * réplicas (fuera del alcance de este servicio), así que aquí solo se
 * soportan: aumentar particiones, y alterar retention.ms / cleanup.policy.
 * Todos los campos son opcionales; se aplica solo lo que venga no-nulo.
 */
public record TopicoActualizarRequest(
        @Positive(message = "particiones debe ser mayor que 0")
        Integer particiones,

        @Positive(message = "retentionMs debe ser mayor que 0")
        Long retentionMs,

        CleanupPolicy cleanupPolicy
) {
}
