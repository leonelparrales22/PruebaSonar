def pyspark_output(spark, df, write_options_dict, param_dict):
    _ = write_options_dict

    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    import threading

    jdbc_user_str = str(param_dict.get("JDBC_USER"))
    root_cert_str = str(param_dict.get("ROOT_CERT"))
    client_cert_str = str(param_dict.get("CLIENT_CERT"))
    key_cert_str = str(param_dict.get("KEY_CERT"))

    df_write = df.filter(F.col("tipo_procesamiento") == "STANDIN_P")
    df_write.persist()

    @F.udf(returnType=StringType())
    def delete_udf(row):
        import psycopg2
        import os
        import stat
        import shutil
        import tempfile

        if not row:
            return
        periodo = getattr(row, "periodo", None)
        id_transaccion = getattr(row, "id_transaccion", None)

        if not (periodo and id_transaccion):
            return
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
            delete_query = """
                    UPDATE pas.movimientos_cuenta
                    SET tipo_procesamiento = 'STANDIN'
                    WHERE periodo = %s
                    AND id_transaccion = %s
            """
            cursor.execute(
                delete_query,
                (
                    periodo,
                    id_transaccion,
                ),
            )
            thread_local.db_connection.commit()
        except Exception as e:
            thread_local.db_connection.rollback()
            return f"ERROR: {str(e)}"
        finally:
            cursor.close()

    df_write.withColumn(
        "delete_status", delete_udf(F.struct([F.col(c) for c in df_write.columns]))
    ).select("delete_status").show(10000)

    df_write.unpersist()
