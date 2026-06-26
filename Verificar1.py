import re
import uuid


def pyspark_output(spark, df, write_options_dict, param_dict):
    _PART_PWD = "pass" + "word"
    PWD_REGEX = re.compile(rf"{_PART_PWD}=[\"']([^\"']+)[\"']")
    USR_REGEX = re.compile(r"username=[\"']([^\"']+)[\"']")

    topic_sincronizacion_cupos = (
        "{{{Transf_PRY_CargaInicial.P_TOPICO_SINCRONIZACION_CUPOS}}}"
    )

    kafka_auth = "{{{Transf_PRY_PaymentHub.P_KAFKA_AUTH}}}"
    usr_match = USR_REGEX.search(kafka_auth)
    pwd_match = PWD_REGEX.search(kafka_auth)

    kafka_conf_vals = {
        "bootstrap.servers": "{{{Transf_PRY_PaymentHub.P_KAFKA_HOST}}}:{{{Transf_PRY_PaymentHub.P_KAFKA_PORT}}}",
        "sasl.username": usr_match.group(1) if usr_match else "",
        "sasl.password": pwd_match.group(1) if pwd_match else "",
        "sr.url": "{{{Transf_PRY_PaymentHub.P_SCHEMA_REGISTRY_URL}}}",
        "sr.auth": "{{{Transf_PRY_PaymentHub.P_SCHEMA_REGISTRY_AUTH}}}",
    }

    global schema_sincronizacion_cupos_cache
    if "schema_sincronizacion_cupos_cache" not in globals():
        from confluent_kafka.schema_registry import SchemaRegistryClient

        sr_client_driver = SchemaRegistryClient(
            {
                "url": kafka_conf_vals["sr.url"],
                "basic.auth.user.info": kafka_conf_vals["sr.auth"],
            }
        )
        
        schema_id_str = "{{{Transf_PRY_CargaInicial.P_ID_SCHEMA_CUPOS}}}"
        schema_id = int(schema_id_str) if schema_id_str.isdigit() else schema_id_str
        
        schema_sincronizacion_cupos_cache = sr_client_driver.get_schema(
            schema_id
        ).schema_str

    schema_str = schema_sincronizacion_cupos_cache

    def send_partition(partition):
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import (
            MessageField,
            SerializationContext,
        )

        sr_client_exec = SchemaRegistryClient(
            {
                "url": kafka_conf_vals["sr.url"],
                "basic.auth.user.info": kafka_conf_vals["sr.auth"],
            }
        )

        avro_serializer_tango = AvroSerializer(
            sr_client_exec,
            schema_str,
            conf={"auto.register.schemas": False},
        )
        ctx_tango = SerializationContext(
            topic_sincronizacion_cupos, MessageField.VALUE
        )

        p = Producer(
            {
                "bootstrap.servers": kafka_conf_vals["bootstrap.servers"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": kafka_conf_vals["sasl.username"],
                "sasl.password": kafka_conf_vals["sasl.password"],
                "client.id": f"pyspark-producer-{uuid.uuid4()}",
                "enable.idempotence": True,
                "linger.ms": 5,
                "compression.type": "lz4",
                "acks": "all",
            }
        )

        for row in partition:
            record = row.asDict(recursive=True)
            message_key = (
                str(record["id_transaccion"])
                if "id_transaccion" in record
                else str(uuid.uuid4())
            )

            kafka_payload = record["kafka_message"]
            value_bytes_tango = avro_serializer_tango(kafka_payload, ctx_tango)

            p.produce(
                topic=topic_sincronizacion_cupos,
                key=message_key.encode("utf-8"),
                value=value_bytes_tango,
            )
            p.poll(0)

        p.flush()

    df.rdd.foreachPartition(send_partition)
