def pyspark_transform(spark, df, param_dict):
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType,
        StructField,
        DecimalType,
        StringType,
        LongType,
    )
    import threading

    jdbc_user_str = str(param_dict.get("JDBC_USER"))
    root_cert_str = str(param_dict.get("ROOT_CERT"))
    client_cert_str = str(param_dict.get("CLIENT_CERT"))
    key_cert_str = str(param_dict.get("KEY_CERT"))

    enrich_schema = StructType(
        [
            StructField("saldo", DecimalType(38, 9)),
            StructField("producto_tipo_cod", StringType()),
            StructField("descripcion_producto", StringType()),
            StructField("codigo_estado_cuenta", StringType()),
            StructField("nemonico_moneda_cuenta", StringType()),
            StructField("desautorizaciones_cuenta", StringType()),
            StructField("orden_registro", LongType()),
            StructField("orden_registro_cuenta_id", LongType()),
        ]
    )

    @F.udf(returnType=enrich_schema)
    def db_lookup_udf(row):
        import psycopg2
        import os
        import stat
        import shutil
        import tempfile
        from decimal import Decimal
        from datetime import date, datetime

        if not row:
            return (None, "ND", None, None, None, None, None, None)

        def get_val(col):
            return getattr(row, col, None)

        cuenta_id = get_val("numero_cuenta_id")
        id_transaccion = get_val("id_transaccion")
        periodo = get_val("periodo")
        tipo_transaccion = get_val("tipo_transaccion")
        valor_transaccion = get_val("valor_transaccion")

        global thread_local
        if "thread_local" not in globals():
            thread_local = threading.local()
        if (
            not hasattr(thread_local, "db_connection")
            or thread_local.db_connection.closed != 0
        ):
            orig_key_path = key_cert_str
            
            if not hasattr(thread_local, "secure_key_path") or not os.path.exists(thread_local.secure_key_path):
                try:
                    fd, temp_path = tempfile.mkstemp(prefix="llave_segura_", suffix=".pkcs8")
                    os.close(fd)
                    shutil.copyfile(orig_key_path, temp_path)
                    os.chmod(temp_path, stat.S_IRUSR | stat.S_IWUSR)
                    thread_local.secure_key_path = temp_path
                except Exception:
                    pass
            
            new_key_path = getattr(thread_local, "secure_key_path", orig_key_path)
            
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
                sslkey=new_key_path,
            )
        if not cuenta_id or not id_transaccion:
            return (None, "ND", None, None, None, None, None, None)
        cursor = thread_local.db_connection.cursor()

        try:

            query_maestro = """
                SELECT saldo_disponible_cuenta, codigo_producto_transaccional_cuenta, 
                       descripcion_producto, codigo_estado_cuenta, 
                       nemonico_moneda_cuenta, desautorizaciones_cuenta
                FROM pas.cuentas_stg 
                WHERE numero_cuenta_id = %s FOR UPDATE
            """
            cursor.execute(query_maestro, (cuenta_id,))
            row_m = cursor.fetchone()

            if not row_m:
                thread_local.db_connection.rollback()
                cursor.close()
                return (None, "ND", None, None, None, None, None, None)
            saldo_base = Decimal(row_m[0]) if row_m[0] is not None else Decimal("0")
            codigo_prod = row_m[1]
            desc_prod = row_m[2]
            est_cta = row_m[3]
            mon_cta = row_m[4]
            desaut_cta = row_m[5]

            if codigo_prod and codigo_prod.startswith("21"):
                tipo_cod = "CACC"
            elif codigo_prod and codigo_prod.startswith("22"):
                tipo_cod = "SVGS"
            else:
                tipo_cod = "ND"
            query_check = """
                SELECT saldo, orden_registro, orden_registro_cuenta_id 
                FROM pas.movimientos_cuenta 
                WHERE periodo = %s AND id_transaccion = %s LIMIT 1
            """
            cursor.execute(query_check, (periodo, id_transaccion))
            row_h = cursor.fetchone()

            if row_h:

                nuevo_saldo = (
                    Decimal(row_h[0]) if row_h[0] is not None else Decimal("0")
                )
                old_orden = row_h[1]
                old_orden_id = row_h[2]
                thread_local.db_connection.rollback()
                cursor.close()
                return (
                    nuevo_saldo,
                    tipo_cod,
                    desc_prod,
                    est_cta,
                    mon_cta,
                    desaut_cta,
                    old_orden,
                    old_orden_id,
                )
            val_tx = (
                Decimal(valor_transaccion)
                if valor_transaccion is not None
                else Decimal("0")
            )
            if tipo_transaccion == "DBIT":
                nuevo_saldo = saldo_base - val_tx
            elif tipo_transaccion == "CRDT":
                nuevo_saldo = saldo_base + val_tx
            else:
                nuevo_saldo = saldo_base
            query_update_stg = "UPDATE pas.cuentas_stg SET saldo_disponible_cuenta = %s, saldo_contable_cuenta = %s WHERE numero_cuenta_id = %s"
            cursor.execute(query_update_stg, (nuevo_saldo, nuevo_saldo, cuenta_id))

            query_update_main = "UPDATE pas.cuentas SET saldo_disponible_cuenta = %s, saldo_contable_cuenta = %s WHERE numero_cuenta_id = %s"
            cursor.execute(query_update_main, (nuevo_saldo, nuevo_saldo, cuenta_id))

            query_orden = "SELECT MIN(orden_registro) FROM pas.movimientos_cuenta WHERE numero_cuenta_id = %s"
            cursor.execute(query_orden, (cuenta_id,))
            row_orden = cursor.fetchone()

            nuevo_orden_registro = (
                (int(row_orden[0]) - 1)
                if (row_orden and row_orden[0] is not None)
                else 999999999
            )

            orden_invertido = 9999999999 - nuevo_orden_registro

            if isinstance(periodo, str):
                p_date = datetime.strptime(periodo, "%Y-%m-%d").date()
            else:
                p_date = periodo
            dias_diff = (p_date - date(1899, 12, 31)).days
            orden_registro_cuenta_id_calc = int(
                f"{dias_diff}{str(orden_invertido).zfill(10)}"
            )

            query_insert = """
                INSERT INTO pas.movimientos_cuenta (
                    banco_beneficiario, banco_ordenante, nombre_beneficiario, nombre_ordenante,
                    descripcion_ordenante, descripcion, descripcion_enriquecida, estado_transaccion,
                    fecha_hora_operacion, fecha_hora_transaccion_contable, fecha_transaccion_contable,
                    identificacion_beneficiario, identificacion_ordenante, valor_transaccion,
                    numero_documento, numero_documento_comprobante, cuenta_beneficiario, cuenta_ordenante,
                    numero_cuenta, numero_cuenta_id, saldo, tipo_transaccion, moneda_origen,
                    fecha_carga, kafka_timestamp_ec, origen, fecha_transaccion_origen, tipo_procesamiento,
                    periodo, id_transaccion, orden_registro, orden_registro_cuenta_id,
                    completar_detalle_enriquecido, tiene_detalle_extendido
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                ) ON CONFLICT (periodo, id_transaccion) DO NOTHING
            """
            cursor.execute(
                query_insert,
                (
                    get_val("banco_beneficiario"),
                    get_val("banco_ordenante"),
                    "",
                    "",
                    get_val("descripcion_ordenante"),
                    get_val("descripcion"),
                    get_val("descripcion"),
                    get_val("estado_transaccion"),
                    get_val("fecha_hora_operacion"),
                    get_val("fecha_hora_transaccion_contable"),
                    get_val("fecha_transaccion_contable"),
                    get_val("identificacion_beneficiario"),
                    get_val("identificacion_ordenante"),
                    val_tx,
                    get_val("numero_documento"),
                    get_val("numero_documento_comprobante")
                    or get_val("numero_documento"),
                    get_val("cuenta_beneficiario"),
                    get_val("cuenta_ordenante"),
                    get_val("numero_cuenta"),
                    cuenta_id,
                    nuevo_saldo,
                    tipo_transaccion,
                    get_val("moneda_origen"),
                    get_val("fecha_carga"),
                    get_val("kafka_timestamp_ec"),
                    get_val("origen"),
                    get_val("fecha_transaccion_origen"),
                    get_val("tipo_procesamiento"),
                    periodo,
                    id_transaccion,
                    nuevo_orden_registro,
                    orden_registro_cuenta_id_calc,
                    True,
                    True,
                ),
            )

            thread_local.db_connection.commit()
            cursor.close()

            return (
                nuevo_saldo,
                tipo_cod,
                desc_prod,
                est_cta,
                mon_cta,
                desaut_cta,
                nuevo_orden_registro,
                orden_registro_cuenta_id_calc,
            )
        except Exception as e:
            thread_local.db_connection.rollback()
            cursor.close()
            raise e

    df_final = (
        df.withColumn(
            "enrich_data", db_lookup_udf(F.struct([F.col(c) for c in df.columns]))
        )
        .select("*", "enrich_data.*")
        .drop("enrich_data")
    )

    return df_final
