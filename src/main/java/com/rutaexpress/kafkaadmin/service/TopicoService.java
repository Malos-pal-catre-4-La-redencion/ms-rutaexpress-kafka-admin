package com.rutaexpress.kafkaadmin.service;

import com.rutaexpress.kafkaadmin.config.KafkaAdminProperties;
import com.rutaexpress.kafkaadmin.exception.ConflictoKafkaException;
import com.rutaexpress.kafkaadmin.exception.KafkaNoDisponibleException;
import com.rutaexpress.kafkaadmin.exception.RecursoNoEncontradoException;
import com.rutaexpress.kafkaadmin.exception.SolicitudInvalidaException;
import com.rutaexpress.kafkaadmin.web.dto.CleanupPolicy;
import com.rutaexpress.kafkaadmin.web.dto.TopicoActualizarRequest;
import com.rutaexpress.kafkaadmin.web.dto.TopicoRequest;
import com.rutaexpress.kafkaadmin.web.dto.TopicoResponse;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Administra tópicos usando el Admin de Kafka directamente (sin pasar por
 * ningún topic en sí): crear, listar, describir, modificar (particiones y
 * configuraciones) y eliminar.
 */
@Service
public class TopicoService {

    private final Admin admin;
    private final KafkaAdminProperties.DefaultsTopico defaults;

    public TopicoService(Admin admin, KafkaAdminProperties properties) {
        this.admin = admin;
        this.defaults = properties.defaultsTopico();
    }

    public TopicoResponse crearTopico(TopicoRequest request) {
        int particiones = request.particiones() != null ? request.particiones() : defaults.particiones();
        short factorReplicacion = request.factorReplicacion() != null
                ? request.factorReplicacion() : defaults.factorReplicacion();

        NewTopic nuevoTopico = new NewTopic(request.nombre(), particiones, factorReplicacion)
                .configs(configsIniciales(request));

        ejecutar(admin.createTopics(List.of(nuevoTopico)).all(), request.nombre());
        return obtenerTopico(request.nombre());
    }

    public List<String> listarTopicos() {
        Set<String> nombres = ejecutar(admin.listTopics().names(), "(listado)");
        return nombres.stream()
                .filter(nombre -> !nombre.startsWith("_")) // oculta tópicos internos de Kafka
                .sorted()
                .collect(Collectors.toList());
    }

    public TopicoResponse obtenerTopico(String nombre) {
        Map<String, TopicDescription> descripciones =
                ejecutar(admin.describeTopics(List.of(nombre)).allTopicNames(), nombre);
        TopicDescription descripcion = descripciones.get(nombre);
        if (descripcion == null) {
            throw new RecursoNoEncontradoException("No existe el tópico '" + nombre + "'");
        }

        List<TopicPartitionInfo> particiones = descripcion.partitions();
        short factorReplicacion = particiones.isEmpty()
                ? 0
                : (short) particiones.get(0).replicas().size();

        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Map<ConfigResource, Config> configs =
                ejecutar(admin.describeConfigs(List.of(recurso)).all(), nombre);
        Map<String, String> configuraciones = configs.get(recurso).entries().stream()
                .filter(entry -> !entry.isDefault())
                .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value, (a, b) -> a, java.util.LinkedHashMap::new));

        return new TopicoResponse(nombre, particiones.size(), factorReplicacion, configuraciones);
    }

    public TopicoResponse actualizarTopico(String nombre, TopicoActualizarRequest request) {
        TopicoResponse actual = obtenerTopico(nombre);

        if (request.particiones() != null) {
            if (request.particiones() <= actual.particiones()) {
                throw new SolicitudInvalidaException(
                        "Kafka no permite reducir particiones; el tópico '" + nombre + "' ya tiene "
                                + actual.particiones() + ".");
            }
            ejecutar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(request.particiones()))).all(),
                    nombre);
        }

        if (request.retentionMs() != null || request.cleanupPolicy() != null) {
            ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
            List<AlterConfigOp> cambios = new java.util.ArrayList<>();
            if (request.retentionMs() != null) {
                cambios.add(new AlterConfigOp(
                        new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(request.retentionMs())),
                        AlterConfigOp.OpType.SET));
            }
            if (request.cleanupPolicy() != null) {
                cambios.add(new AlterConfigOp(
                        new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, request.cleanupPolicy().valorKafka()),
                        AlterConfigOp.OpType.SET));
            }
            ejecutar(admin.incrementalAlterConfigs(Map.of(recurso, cambios)).all(), nombre);
        }

        return obtenerTopico(nombre);
    }

    public void eliminarTopico(String nombre) {
        ejecutar(admin.deleteTopics(List.of(nombre)).all(), nombre);
    }

    private Map<String, String> configsIniciales(TopicoRequest request) {
        String retentionMs = String.valueOf(request.retentionMs() != null ? request.retentionMs() : defaults.retentionMs());
        CleanupPolicy cleanupPolicy = request.cleanupPolicy();
        String cleanupPolicyValor = cleanupPolicy != null ? cleanupPolicy.valorKafka() : defaults.cleanupPolicy();
        return Map.of(
                TopicConfig.RETENTION_MS_CONFIG, retentionMs,
                TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicyValor
        );
    }

    /**
     * Toda llamada al Admin de Kafka es asíncrona (KafkaFuture) y lanza las
     * excepciones reales envueltas en ExecutionException. Este método las
     * desenvuelve y las traduce a las excepciones de negocio que entiende el
     * GlobalExceptionHandler.
     */
    private <T> T ejecutar(KafkaFuture<T> future, String nombreTopico) {
        try {
            return future.get();
        } catch (ExecutionException ex) {
            Throwable causa = ex.getCause();
            if (causa instanceof TopicExistsException) {
                throw new ConflictoKafkaException("Ya existe un tópico llamado '" + nombreTopico + "'");
            }
            if (causa instanceof UnknownTopicOrPartitionException) {
                throw new RecursoNoEncontradoException("No existe el tópico '" + nombreTopico + "'");
            }
            if (causa instanceof InvalidPartitionsException
                    || causa instanceof InvalidReplicationFactorException
                    || causa instanceof InvalidConfigurationException) {
                throw new SolicitudInvalidaException(causa.getMessage());
            }
            if (causa instanceof TimeoutException) {
                throw new KafkaNoDisponibleException("Kafka no respondió a tiempo", causa);
            }
            throw new KafkaNoDisponibleException("Error inesperado al comunicarse con Kafka", causa);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaNoDisponibleException("La operación contra Kafka fue interrumpida", ex);
        }
    }
}
