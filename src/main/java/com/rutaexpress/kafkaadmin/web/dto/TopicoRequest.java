package com.rutaexpress.kafkaadmin.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

/**
 * Request de creación. Los campos numéricos y cleanupPolicy son opcionales:
 * si no se envían, el servicio aplica los defaults de
 * rutaexpress.kafka-admin.defaults-topico (3 particiones, 3 réplicas,
 * retention de 7 días, cleanup.policy=delete).
 */
public record TopicoRequest(
        @NotBlank(message = "El nombre del tópico es obligatorio")
        @Size(max = 249, message = "El nombre del tópico no puede superar 249 caracteres")
        String nombre,

        @Positive(message = "particiones debe ser mayor que 0")
        Integer particiones,

        @Positive(message = "factorReplicacion debe ser mayor que 0")
        Short factorReplicacion,

        @Positive(message = "retentionMs debe ser mayor que 0")
        Long retentionMs,

        CleanupPolicy cleanupPolicy
) {
}
