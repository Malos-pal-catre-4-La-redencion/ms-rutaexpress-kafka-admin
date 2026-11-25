# ms-rutaexpress-kafka-admin

Administración de Kafka para RutaExpress (EP5/EP6, DSY1107). Este repositorio contiene:

- `docker/` — Zookeeper + 3 brokers Kafka + Kafka-UI con Docker Compose.
- Microservicio Spring Boot que crea, lista, modifica y elimina tópicos, y lista consumer groups con su lag, por REST.

## Arquitectura

```
Angular (MSAL) → API Gateway (/api/admin/*) → BFF (solo rol Admin) → ms-kafka-admin → Kafka (AdminClient)
```

Este servicio es **interno**, igual que shipments, catalog y rabbit-admin: no valida JWT,
porque el BFF ya lo valida y exige rol `Admin` en toda la ruta de administración.
En EC2 su puerto (8085) no debe estar abierto a internet.

No produce ni consume mensajes de negocio — eso lo hacen catalog y shipments. Este
servicio solo administra la plataforma (tópicos, particiones, configuraciones, grupos).

## Requisitos

- Java 17 y Maven 3.9+
- El clúster Kafka en marcha (`docker/docker-compose.yml`)

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `SERVER_PORT` | `8085` | Puerto del servicio |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9093,localhost:9094` | Lista de brokers |

```bash
cd docker && cp .env.example .env && docker compose up -d
mvn spring-boot:run
```

Salud del servicio: `GET http://localhost:8085/actuator/health`

## Endpoints

### Tópicos — `/topics`

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/topics` | Crea un tópico. Body: `nombre` (obligatorio), `particiones`, `factorReplicacion`, `retentionMs`, `cleanupPolicy` (`DELETE`/`COMPACT`) — todos opcionales, se aplican los defaults de `rutaexpress.kafka-admin.defaults-topico` (3 particiones, 3 réplicas, retention 7 días, `delete`). |
| `GET` | `/topics` | Lista los nombres de tópicos (oculta los internos de Kafka, prefijo `_`). |
| `GET` | `/topics/{nombre}` | Detalle: particiones, factor de replicación, configuraciones no-default. |
| `PATCH` | `/topics/{nombre}` | Modifica: `particiones` (solo se puede **aumentar**, Kafka no permite reducir), `retentionMs`, `cleanupPolicy`. |
| `DELETE` | `/topics/{nombre}` | Elimina el tópico. |

### Consumer groups — `/consumer-groups` (solo lectura)

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/consumer-groups` | Lista todos los grupos con su estado. |
| `GET` | `/consumer-groups/{groupId}` | Detalle por partición: offset confirmado, offset final del log y **lag** (mensajes pendientes de procesar). |

## Tópicos de RutaExpress

Definidos y poblados desde catalog/shipments (ver sus propios README):

| Tópico | Productor | Consumidor | Consumer group |
|---|---|---|---|
| `rutaexpress.eventos.envios` | shipments | catalog | `catalog-analitica-envios` |
| `rutaexpress.eventos.vehiculos` | catalog | shipments | `shipments-analitica-vehiculos` |

Kafka complementa la integración RabbitMQ ya existente (comandos/eventos de negocio);
estos tópicos son para analítica/streaming, no reemplazan esa orquestación.
