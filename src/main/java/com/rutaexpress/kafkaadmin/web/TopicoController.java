package com.rutaexpress.kafkaadmin.web;

import com.rutaexpress.kafkaadmin.service.TopicoService;
import com.rutaexpress.kafkaadmin.web.dto.TopicoActualizarRequest;
import com.rutaexpress.kafkaadmin.web.dto.TopicoRequest;
import com.rutaexpress.kafkaadmin.web.dto.TopicoResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Este servicio no valida JWT por su cuenta: solo lo llama el BFF, que ya
 * exige rol Admin para toda la ruta de administración (igual que rabbit-admin).
 */
@RestController
@RequestMapping("/topics")
public class TopicoController {

    private final TopicoService service;

    public TopicoController(TopicoService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<TopicoResponse> crear(@Valid @RequestBody TopicoRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(service.crearTopico(request));
    }

    @GetMapping
    public List<String> listar() {
        return service.listarTopicos();
    }

    @GetMapping("/{nombre}")
    public TopicoResponse obtener(@PathVariable String nombre) {
        return service.obtenerTopico(nombre);
    }

    @PatchMapping("/{nombre}")
    public TopicoResponse actualizar(@PathVariable String nombre, @Valid @RequestBody TopicoActualizarRequest request) {
        return service.actualizarTopico(nombre, request);
    }

    @DeleteMapping("/{nombre}")
    public ResponseEntity<Void> eliminar(@PathVariable String nombre) {
        service.eliminarTopico(nombre);
        return ResponseEntity.noContent().build();
    }
}
