package com.rutaexpress.kafkaadmin.service;

import com.rutaexpress.kafkaadmin.exception.KafkaNoDisponibleException;
import com.rutaexpress.kafkaadmin.exception.RecursoNoEncontradoException;
import com.rutaexpress.kafkaadmin.web.dto.GrupoConsumidorDetalleResponse;
import com.rutaexpress.kafkaadmin.web.dto.GrupoConsumidorResponse;
import com.rutaexpress.kafkaadmin.web.dto.ParticionConsumidaResponse;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

/**
 * Describe consumer groups y calcula el lag (mensajes pendientes por
 * procesar) comparando el offset confirmado por el grupo contra el offset
 * final (log-end-offset) real de cada partición.
 */
@Service
public class GrupoConsumidorService {

    private final Admin admin;

    public GrupoConsumidorService(Admin admin) {
        this.admin = admin;
    }

    public List<GrupoConsumidorResponse> listarGrupos() {
        var listados = ejecutar(admin.listConsumerGroups().all(), "(listado)");
        return listados.stream()
                .map(g -> new GrupoConsumidorResponse(g.groupId(), g.state().map(Enum::toString).orElse("DESCONOCIDO")))
                .sorted((a, b) -> a.groupId().compareTo(b.groupId()))
                .collect(Collectors.toList());
    }

    public GrupoConsumidorDetalleResponse describirGrupo(String groupId) {
        Map<String, ConsumerGroupDescription> descripciones =
                ejecutar(admin.describeConsumerGroups(List.of(groupId)).all(), groupId);
        ConsumerGroupDescription descripcion = descripciones.get(groupId);
        // describeConsumerGroups nunca lanza "no encontrado": para un groupId que
        // nunca existió, Kafka devuelve una descripción vacía en estado DEAD.
        if (descripcion == null || descripcion.state() == org.apache.kafka.common.ConsumerGroupState.DEAD) {
            throw new RecursoNoEncontradoException("No existe el consumer group '" + groupId + "'");
        }

        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsetsConfirmados =
                ejecutar(admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(), groupId);

        if (offsetsConfirmados.isEmpty()) {
            return new GrupoConsumidorDetalleResponse(groupId, descripcion.state().toString(),
                    descripcion.members().size(), List.of());
        }

        Map<TopicPartition, OffsetSpec> requestFinales = offsetsConfirmados.keySet().stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> finales =
                ejecutar(admin.listOffsets(requestFinales).all(), groupId);

        List<ParticionConsumidaResponse> particiones = offsetsConfirmados.entrySet().stream()
                .map(entry -> {
                    TopicPartition tp = entry.getKey();
                    long actual = entry.getValue().offset();
                    long finalLog = finales.get(tp).offset();
                    return new ParticionConsumidaResponse(tp.topic(), tp.partition(), actual, finalLog, finalLog - actual);
                })
                .sorted((a, b) -> a.topico().equals(b.topico())
                        ? Integer.compare(a.particion(), b.particion())
                        : a.topico().compareTo(b.topico()))
                .collect(Collectors.toList());

        return new GrupoConsumidorDetalleResponse(groupId, descripcion.state().toString(),
                descripcion.members().size(), particiones);
    }

    private <T> T ejecutar(KafkaFuture<T> future, String contexto) {
        try {
            return future.get();
        } catch (ExecutionException ex) {
            throw new KafkaNoDisponibleException("Error al consultar Kafka para '" + contexto + "'", ex.getCause());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaNoDisponibleException("La operación contra Kafka fue interrumpida", ex);
        }
    }
}
