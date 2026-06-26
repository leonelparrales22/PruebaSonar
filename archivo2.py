def pyspark_transform(spark, df, param_dict):
    _ = spark

    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        ArrayType,
    )
    import threading

    jdbc_user_str = str(param_dict.get("JDBC_USER"))
    root_cert_str = str(param_dict.get("ROOT_CERT"))
    client_cert_str = str(param_dict.get("CLIENT_CERT"))
    key_cert_str = str(param_dict.get("KEY_CERT"))

    enrich_schema = ArrayType(
        StructType(
            [
                StructField("id_transaccion", StringType()),
                StructField("numero_cuenta_id", StringType()),
                StructField("fecha_transaccion_contable", StringType()),
                StructField("contrapartida", StringType()),
                StructField("orden_registro", StringType()),
                StructField("orden_registro_cuenta_id", StringType()),
                StructField("numero_documento", StringType()),
                StructField("numero_comprobante", StringType()),
                StructField("fecha_hora_transaccion_contable", StringType()),
                StructField("descripcion", StringType()),
                StructField("descripcion_enriquecida", StringType()),
                StructField("valor_transaccion", StringType()),
                StructField("moneda_origen", StringType()),
                StructField("tipo_transaccion", StringType()),
                StructField("numero_documento_comprobante", StringType()),
                StructField("saldo", StringType()),
                StructField("codigo_agencia", StringType()),
                StructField("agencia", StringType()),
                StructField("origen", StringType()),
                StructField("codigo_cif_ordenante", StringType()),
                StructField("identificacion_ordenante", StringType()),
                StructField("nombre_ordenante", StringType()),
                StructField("cuenta_ordenante", StringType()),
                StructField("banco_ordenante", StringType()),
                StructField("pais_ordenante", StringType()),
                StructField("codigo_cif_beneficiario", StringType()),
                StructField("identificacion_beneficiario", StringType()),
                StructField("nombre_beneficiario", StringType()),
                StructField("cuenta_beneficiario", StringType()),
                StructField("banco_beneficiario", StringType()),
                StructField("pais_beneficiario", StringType()),
                StructField("estado_transaccion", StringType()),
                StructField("descripcion_ordenante", StringType()),
                StructField("descripcion_servicio", StringType()),
                StructField("fecha_transaccion_origen", StringType()),
                StructField("periodo", StringType()),
                StructField("fecha_carga", StringType()),
                StructField("tiene_detalle_extendido", StringType()),
                StructField("numero_cuenta", StringType()),
                StructField("fecha_hora_operacion", StringType()),
                StructField("kafka_timestamp_ec", StringType()),
                StructField("detalle_extendido_json", StringType()),
                StructField("codigo_usuario_transaccion", StringType()),
                StructField("usuario_transaccion", StringType()),
                StructField("codigo_transaccion", StringType()),
                StructField("numero_contrapartida_id", StringType()),
                StructField("fecha_proceso_nrt", StringType()),
                StructField("completar_detalle_enriquecido", StringType()),
                StructField("tipo_procesamiento", StringType()),
                StructField("numero_comprobante_reposting", StringType()),
            ]
        )
    )

    @F.udf(returnType=enrich_schema)
    def query_and_delete_udf(row):
        import psycopg2
        import os
        import stat
        import shutil
        import tempfile

        if not row:
            return []
        periodo = getattr(row, "periodo", None)
        periodo_tope = getattr(row, "periodo_tope", None)

        if not (periodo and periodo_tope):
            return []
        global thread_local
        if "thread_local" not in globals():
            thread_local = threading.local()
        if (
            not hasattr(thread_local, "db_connection")
            or thread_local.db_connection.closed != 0
        ):
            if not hasattr(thread_local, "secure_key_path") or not os.path.exists(
                thread_local.secure_key_path
            ):
                try:
                    fd, temp_path = tempfile.mkstemp(prefix="llave_", suffix=".pkcs8")
                    os.close(fd)
                    shutil.copyfile(key_cert_str, temp_path)
                    os.chmod(temp_path, stat.S_IRUSR | stat.S_IWUSR)
                    thread_local.secure_key_path = temp_path
                except Exception:
                    pass
            key_path = getattr(thread_local, "secure_key_path", key_cert_str)

            jdbc_url = "{{{JDBC_PG_DATA_ACP_REPLICA}}}"
            clean_url = jdbc_url.replace("jdbc:postgresql://", "").split("?")[0]
            host_port, dbname = clean_url.split("/")
            host, port = host_port.split(":")

            thread_local.db_connection = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=jdbc_user_str,
                sslmode="verify-ca",
                sslrootcert=root_cert_str,
                sslcert=client_cert_str,
                sslkey=key_path,
            )
        cursor = thread_local.db_connection.cursor()

        try:
            query = """
                WITH cuentas_docs AS 
                    (
                    SELECT
                        mc.numero_cuenta_id,
                        mc.numero_documento,
                        mc.numero_comprobante_reposting
                    FROM pas.movimientos_cuenta mc
                    WHERE mc.periodo between %s AND  %s  AND mc.tipo_procesamiento IN ('STANDIN_P','STANDIN')
                ),
                reposting_bancs AS 
                    (
                    SELECT 
                        mc.id_transaccion,
                        mc.numero_cuenta_id,
                        TO_CHAR(mc.fecha_transaccion_contable, 'YYYY-MM-DD') as fecha_transaccion_contable,
                        mc.contrapartida,
                        mc.orden_registro,
                        mc.orden_registro_cuenta_id,
                        mc.numero_documento,
                        mc.numero_comprobante,
                        TO_CHAR(mc.fecha_hora_transaccion_contable, 'YYYY-MM-DD HH24:MI:SS') as fecha_hora_transaccion_contable,
                        mc.descripcion,
                        mc.descripcion_enriquecida,
                        mc.valor_transaccion,
                        mc.moneda_origen,
                        mc.tipo_transaccion,
                        mc.numero_documento_comprobante,
                        mc.saldo,
                        mc.codigo_agencia,
                        mc.agencia,
                        mc.origen,
                        mc.codigo_cif_ordenante,
                        mc.identificacion_ordenante,
                        mc.nombre_ordenante,
                        mc.cuenta_ordenante,
                        mc.banco_ordenante,
                        mc.pais_ordenante,
                        mc.codigo_cif_beneficiario,
                        mc.identificacion_beneficiario,
                        mc.nombre_beneficiario,
                        mc.cuenta_beneficiario,
                        mc.banco_beneficiario,
                        mc.pais_beneficiario,
                        mc.estado_transaccion,
                        mc.descripcion_ordenante,
                        mc.descripcion_servicio,
                        TO_CHAR(mc.fecha_transaccion_origen, 'YYYY-MM-DD') as fecha_transaccion_origen,
                        TO_CHAR(mc.periodo, 'YYYY-MM-DD') as periodo,
                        TO_CHAR(mc.fecha_carga, 'YYYY-MM-DD HH24:MI:SS') as fecha_carga,
                        mc.tiene_detalle_extendido,
                        mc.numero_cuenta,
                        TO_CHAR(mc.fecha_hora_operacion, 'YYYY-MM-DD HH24:MI:SS') as fecha_hora_operacion,
                        TO_CHAR(mc.kafka_timestamp_ec, 'YYYY-MM-DD HH24:MI:SS.MS') as kafka_timestamp_ec,
                        mc.detalle_extendido_json,
                        mc.codigo_usuario_transaccion,
                        mc.usuario_transaccion,
                        mc.codigo_transaccion,
                        mc.numero_contrapartida_id,
                        TO_CHAR(mc.fecha_proceso_nrt, 'YYYY-MM-DD HH24:MI:SS.MS') as fecha_proceso_nrt,
                        mc.completar_detalle_enriquecido,
                        mc.tipo_procesamiento,
                        mc.numero_comprobante_reposting,
                        u.numero_documento as numero_comprobante_standin
                FROM pas.movimientos_cuenta mc
                INNER JOIN cuentas_docs as u 
                    on (mc.numero_cuenta_id = u.numero_cuenta_id AND mc.numero_documento = u.numero_comprobante_reposting)
                    WHERE mc.periodo between %s AND  %s AND mc.tipo_procesamiento IS NULL
                ),
                simil_cuentas_docs AS (
                    SELECT numero_cuenta_id AS cuenta, numero_documento as documento FROM reposting_bancs
                    UNION 
                    SELECT numero_cuenta_id AS cuenta, numero_comprobante_standin as documento FROM reposting_bancs
                )
                SELECT 
                        mc.id_transaccion,
                        mc.numero_cuenta_id,
                        TO_CHAR(mc.fecha_transaccion_contable, 'YYYY-MM-DD') as fecha_transaccion_contable,
                        mc.contrapartida,
                        mc.orden_registro,
                        mc.orden_registro_cuenta_id,
                        mc.numero_documento,
                        mc.numero_comprobante,
                        TO_CHAR(mc.fecha_hora_transaccion_contable, 'YYYY-MM-DD HH24:MI:SS') as fecha_hora_transaccion_contable,
                        mc.descripcion,
                        mc.descripcion_enriquecida,
                        mc.valor_transaccion,
                        mc.moneda_origen,
                        mc.tipo_transaccion,
                        mc.numero_documento_comprobante,
                        mc.saldo,
                        mc.codigo_agencia,
                        mc.agencia,
                        mc.origen,
                        mc.codigo_cif_ordenante,
                        mc.identificacion_ordenante,
                        mc.nombre_ordenante,
                        mc.cuenta_ordenante,
                        mc.banco_ordenante,
                        mc.pais_ordenante,
                        mc.codigo_cif_beneficiario,
                        mc.identificacion_beneficiario,
                        mc.nombre_beneficiario,
                        mc.cuenta_beneficiario,
                        mc.banco_beneficiario,
                        mc.pais_beneficiario,
                        mc.estado_transaccion,
                        mc.descripcion_ordenante,
                        mc.descripcion_servicio,
                        TO_CHAR(mc.fecha_transaccion_origen, 'YYYY-MM-DD') as fecha_transaccion_origen,
                        TO_CHAR(mc.periodo, 'YYYY-MM-DD') as periodo,
                        TO_CHAR(mc.fecha_carga, 'YYYY-MM-DD HH24:MI:SS') as fecha_carga,
                        mc.tiene_detalle_extendido,
                        mc.numero_cuenta,
                        TO_CHAR(mc.fecha_hora_operacion, 'YYYY-MM-DD HH24:MI:SS') as fecha_hora_operacion,
                        TO_CHAR(mc.kafka_timestamp_ec, 'YYYY-MM-DD HH24:MI:SS.MS') as kafka_timestamp_ec,
                        mc.detalle_extendido_json,
                        mc.codigo_usuario_transaccion,
                        mc.usuario_transaccion,
                        mc.codigo_transaccion,
                        mc.numero_contrapartida_id,
                        TO_CHAR(mc.fecha_proceso_nrt, 'YYYY-MM-DD HH24:MI:SS.MS') as fecha_proceso_nrt,
                        mc.completar_detalle_enriquecido,
                        mc.tipo_procesamiento,
                        mc.numero_comprobante_reposting
                FROM pas.movimientos_cuenta mc
                INNER JOIN simil_cuentas_docs as u 
                    on (mc.numero_cuenta_id = u.cuenta AND mc.numero_documento = u.documento)
                    WHERE mc.periodo between %s AND  %s 
            """

            cursor.execute(
                query,
                (periodo_tope, periodo, periodo_tope, periodo, periodo_tope, periodo),
            )

            rows = []
            while True:
                row_result = cursor.fetchone()
                if not row_result:
                    break
                rows.append(row_result)
            thread_local.db_connection.commit()
            return rows
        except Exception as e:
            thread_local.db_connection.rollback()
            raise e
        finally:
            cursor.close()

    df_final = (
        df.withColumn(
            "enrich_data",
            query_and_delete_udf(F.struct([F.col(c) for c in df.columns])),
        )
        .withColumn("enrich_row", F.explode("enrich_data"))
        .select("enrich_row.*")
    )
    return df_final
