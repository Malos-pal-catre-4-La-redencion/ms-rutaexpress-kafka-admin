package com.rutaexpress.kafkaadmin.web.dto;

import java.util.List;

public record GrupoConsumidorDetalleResponse(
        String groupId,
        String estado,
        int miembrosActivos,
        List<ParticionConsumidaResponse> particiones
) {
}
