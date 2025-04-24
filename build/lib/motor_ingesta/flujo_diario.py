import json
from datetime import timedelta
from loguru import logger
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from motor_ingesta.agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto
from motor_ingesta.motor_ingesta import MotorIngesta


class FlujoDiario:

    def __init__(self, config_file: str):
        """
        Completa la documentación
        :param config_file:
        """
        self.spark = SparkSession.builder.getOrCreate()
        with open(config_file, 'r') as f:
            self.config = json.load(f)

    def procesa_diario(self, data_file: str):
        """
        Completa la documentación
        :param data_file:
        :return:
        """

        try:

            motor_ingesta = MotorIngesta(self.config)
            flights_df = motor_ingesta.ingesta_fichero(data_file)

            # Paso 1. Invocamos al método para añadir la hora de salida UTC
            flights_with_utc =   aniade_hora_utc(self.spark, flights_df)


            dia_actual = flights_df.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)
            try:
                flights_previo = self.spark.read.table(self.config["output_table"]).where(F.col("FlightDate") == F.lit(dia_previo))
                logger.info(f"Leída partición del día {dia_previo} con éxito")
            except Exception as e:
                logger.info(f"No se han podido leer datos del día {dia_previo}: {str(e)}")
                flights_previo = None

            if flights_previo:
                flights_with_utc = flights_with_utc \
                    .withColumn("FlightTime_next", F.lit(None).cast("timestamp")) \
                    .withColumn("Airline_next", F.lit(None).cast("string")) \
                    .withColumn("diff_next", F.lit(None).cast("long"))

                flights_with_utc = flights_with_utc.select(flights_previo.columns)

                df_unido = flights_previo.unionByName(flights_with_utc)

                df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
                df_unido = self.spark.read.table("tabla_provisional")

            else:
                df_unido = flights_with_utc           # lo dejamos como está

            # Paso 3. Invocamos al método para añadir información del vuelo siguiente
            df_with_next_flight = aniade_intervalos_por_aeropuerto(df_unido)

            df_with_next_flight \
                .coalesce(self.config["output_partitions"]) \
                .write\
                .mode("overwrite")\
                .option("partitionOverwriteMode", "dynamic")\
                .partitionBy("FlightDate")\
                .saveAsTable(self.config["output_table"])


            # Borrar la tabla provisional si la hubiéramos creado
            self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")

        except Exception as e:
            logger.error(f"No se pudo escribir la tabla del fichero {data_file}")
            raise e


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()   # sólo si lo ejecutas localmente
    flujo = FlujoDiario(str(Path(__file__).parent.parent) + "/config/config.json")
    flujo.procesa_diario(flujo.config["local"]["path_json_procesa_diario_main_flujo_diario"])

    # Recuerda que puedes crear el wheel ejecutando en la línea de comandos: python setup.py bdist_wheel
