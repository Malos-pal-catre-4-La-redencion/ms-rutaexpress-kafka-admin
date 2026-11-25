package com.rutaexpress.kafkaadmin.web.dto;

/**
 * Estado de consumo de una partición dentro de un consumer group.
 * lag = offsetFinal - offsetActual: cuántos mensajes aún no se han procesado.
 */
public record ParticionConsumidaResponse(
        String topico,
        int particion,
        long offsetActual,
        long offsetFinal,
        long lag
) {
}
