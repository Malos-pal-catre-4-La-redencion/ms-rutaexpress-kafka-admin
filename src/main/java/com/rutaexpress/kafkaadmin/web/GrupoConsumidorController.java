package com.rutaexpress.kafkaadmin.web;

import com.rutaexpress.kafkaadmin.service.GrupoConsumidorService;
import com.rutaexpress.kafkaadmin.web.dto.GrupoConsumidorDetalleResponse;
import com.rutaexpress.kafkaadmin.web.dto.GrupoConsumidorResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Solo lectura: lista los consumer groups y, por cada uno, el lag (mensajes
 * pendientes) por partición — lo mismo que se ve en Kafka-UI, expuesto acá
 * como API propia para el BFF/frontend.
 */
@RestController
@RequestMapping("/consumer-groups")
public class GrupoConsumidorController {

    private final GrupoConsumidorService service;

    public GrupoConsumidorController(GrupoConsumidorService service) {
        this.service = service;
    }

    @GetMapping
    public List<GrupoConsumidorResponse> listar() {
        return service.listarGrupos();
    }

    @GetMapping("/{groupId}")
    public GrupoConsumidorDetalleResponse describir(@PathVariable String groupId) {
        return service.describirGrupo(groupId);
    }
}
