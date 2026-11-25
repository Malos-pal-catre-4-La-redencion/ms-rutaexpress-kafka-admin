package com.rutaexpress.kafkaadmin.config;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Único bean que este servicio necesita del lado Kafka: el AdminClient que
 * usa KafkaAdminService para crear/listar/modificar/eliminar tópicos y para
 * describir consumer groups. No produce ni consume mensajes.
 */
@Configuration
@EnableConfigurationProperties(KafkaAdminProperties.class)
public class KafkaAdminClientConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public Admin kafkaAdminClient() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }
}
