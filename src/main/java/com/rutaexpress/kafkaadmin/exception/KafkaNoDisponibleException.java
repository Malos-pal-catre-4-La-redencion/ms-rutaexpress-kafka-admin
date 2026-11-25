package com.rutaexpress.kafkaadmin.exception;

public class KafkaNoDisponibleException extends RuntimeException {

    public KafkaNoDisponibleException(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }
}
