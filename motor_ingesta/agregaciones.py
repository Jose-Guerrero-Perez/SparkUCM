from pathlib import Path

from pyspark.sql import SparkSession, DataFrame as DF, functions as F, Window
import pandas as pd


def aniade_hora_utc(spark: SparkSession, df: DF) -> DF:
    """
    Completa la documentación
    :param spark:
    :param df:
    :param fichero_timezones:
    :return:
    """

    path_timezones = str(Path(__file__).parent) + "/resources/timezones.csv"
    timezones_pd = pd.read_csv(path_timezones)
    timezones_df = spark.createDataFrame(timezones_pd)

    df_with_tz = df.join(timezones_df,
                         df["Origin"] == timezones_df["iata_code"],
                        how="left_outer")


    df_with_castedHour = df_with_tz.withColumn("castedHour",
                                               F.lpad(F.col("DepTime").cast("string"),4,"0")
                                               )
    df_with_flight_time = df_with_castedHour.withColumn(
                                                        "FlightTime",
                                                        F.concat(
                                                            F.col("FlightDate").cast("string"),
                                                            F.lit(" "),
                                                            F.col("castedHour").substr(1,2),
                                                            F.lit(":"),
                                                            F.col("castedHour").substr(3, 2),
                                                            F.lit(":00")
                                                        ).cast("timestamp")
                                                        )

    df_with_flight_time = df_with_flight_time.withColumn("FlightTime",
                                                         F.to_utc_timestamp(F.col("FlightTime"), F.col("iana_tz"))
                                                         )

    columnas_para_borrar = timezones_df.columns + ["castedHour"]
    df_with_flight_time = df_with_flight_time.drop(*columnas_para_borrar)


    return df_with_flight_time


def aniade_intervalos_por_aeropuerto(df: DF) -> DF:
    """
    Completa la documentación
    :param df:
    :return:
    """

    df = df.withColumn("Par", F.struct("FlightTime", "Reporting_Airline"))

    w = Window().partitionBy("Origin").orderBy("FlightTime")

    df = df.withColumn("Par_next", F.lag("Par", -1).over(w))

    df = df.withColumn("FlightTime_next", F.col("Par_next").getField("FlightTime"))
    df = df.withColumn("Airline_next", F.col("Par_next").getField("Reporting_Airline"))
    df = df.withColumn("diff_next", F.col("FlightTime_next").cast("long") - F.col("FlightTime").cast("long"))

    df_with_next_flight =  df.drop("par", "par_next")

    return df_with_next_flight
