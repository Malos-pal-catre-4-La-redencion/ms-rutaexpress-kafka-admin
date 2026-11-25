package com.rutaexpress.kafkaadmin.web.dto;

import java.util.Map;

public record TopicoResponse(
        String nombre,
        int particiones,
        short factorReplicacion,
        Map<String, String> configuraciones
) {
}
