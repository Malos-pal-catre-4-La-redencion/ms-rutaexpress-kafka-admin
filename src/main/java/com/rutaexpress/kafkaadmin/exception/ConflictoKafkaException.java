package com.rutaexpress.kafkaadmin.exception;

public class ConflictoKafkaException extends RuntimeException {

    public ConflictoKafkaException(String mensaje) {
        super(mensaje);
    }
}
