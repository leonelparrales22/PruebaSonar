import re
import uuid


def pyspark_output(spark, df, write_options_dict, param_dict):
    _PART_PWD = "pass" + "word"
    PWD_REGEX = re.compile(rf"{_PART_PWD}=[\"']([^\"']+)[\"']")
    USR_REGEX = re.compile(r"username=[\"']([^\"']+)[\"']")

    topic_tango = "{{{Transf_PRY_PaymentHub.P_TOPICO_SINCRONIZACION_TANGO}}}"

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

    global schema_tango_cache
    if "schema_tango_cache" not in globals():
        from confluent_kafka.schema_registry import SchemaRegistryClient

        sr_client_driver = SchemaRegistryClient(
            {
                "url": kafka_conf_vals["sr.url"],
                "basic.auth.user.info": kafka_conf_vals["sr.auth"],
            }
        )
        schema_tango_cache = sr_client_driver.get_schema(
            {{{Transf_PRY_PaymentHub.P_ID_SCHEMA_SALDOS}}}
        )

    schema_str = schema_tango_cache

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
        ctx_tango = SerializationContext(topic_tango, MessageField.VALUE)

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
                topic=topic_tango,
                key=message_key.encode("utf-8"),
                value=value_bytes_tango,
            )

            p.poll(0)
        p.flush()

    df.rdd.foreachPartition(send_partition)

