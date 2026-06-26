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
                StructField("reposting_completo", StringType()),
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

        def get(c):
            return getattr(row, c, None)

        producto_ordenante = get("producto_ordenante")
        producto_beneficiario = get("producto_beneficiario")
        numero_documento_original = get("numero_documento_original")
        periodo = get("periodo")
        periodo_tope = get("periodo_tope")
        guid_ordenante = get("guid_ordenante")
        guid_beneficiario = get("guid_beneficiario")

        if not (
            producto_ordenante and producto_beneficiario and numero_documento_original
        ):
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
                WITH unicidad AS 
                    (
                    SELECT 
                        u.id_transaccion,
                        u.numero_cuenta_id as cuenta_ordenante,
                        ltrim(u.cuenta_beneficiario, '0') as cuenta_beneficiario,
                        u.numero_documento,
                        %s as numero_documento_original
                    FROM pas.movimientos_cuenta as u
                    where u.periodo >= %s AND u.periodo <= %s
                    AND (u.id_transaccion = %s )
                    ),
                cuentas_docs AS (
                    SELECT cuenta_ordenante AS cuenta, numero_documento as documento FROM unicidad
                    UNION ALL
                    SELECT cuenta_beneficiario AS cuenta, numero_documento as documento FROM unicidad
                    UNION ALL
                    SELECT cuenta_ordenante AS cuenta, numero_documento_original as documento FROM unicidad
                    UNION ALL
                    SELECT cuenta_beneficiario AS cuenta, numero_documento_original as documento FROM unicidad 
                ),           
                reposting_bancs AS (
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
                    INNER JOIN cuentas_docs as u 
                        on (mc.numero_cuenta_id = u.cuenta AND mc.numero_documento = u.documento)
                    WHERE mc.periodo >= %s AND mc.periodo <= %s
                ) 
                SELECT 
                    mc.*,
                    CASE 
                    WHEN EXISTS ( SELECT 1 FROM reposting_bancs rb WHERE rb.tipo_procesamiento IS NULL )
                    AND EXISTS ( SELECT 1 FROM reposting_bancs rb WHERE rb.tipo_procesamiento = 'STANDIN')
                    THEN 'MATCH' 
                    ELSE 'NO MATCH' END AS reposting_completo
                FROM reposting_bancs as mc
            """

            cursor.execute(
                query,
                (
                    numero_documento_original,
                    periodo_tope,
                    periodo,
                    guid_ordenante,
                    periodo_tope,
                    periodo,
                ),
            )

            rows = []
            while True:
                row_result = cursor.fetchone()
                if not row_result:
                    break
                rows.append(row_result)
            cursor.execute(
                """
                UPDATE pas.movimientos_cuenta
                SET numero_comprobante_reposting = %s, estado_transaccion = 10
                WHERE periodo >= %s 
                    AND (id_transaccion = %s OR id_transaccion = %s)
                    AND tipo_procesamiento = 'STANDIN'
                """,
                (
                    numero_documento_original,
                    periodo_tope,
                    guid_ordenante,
                    guid_beneficiario,
                ),
            )

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
