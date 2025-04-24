import json
from collections import namedtuple
from pathlib import Path
from motor_ingesta.motor_ingesta import MotorIngesta
from motor_ingesta.agregaciones import aniade_intervalos_por_aeropuerto, aniade_hora_utc
from pyspark.sql import functions as F


def test_aplana(spark):
    """
    Testea que el aplanado se haga correctamente con un DF creado ad-hoc
    :param spark: SparkSession configurada localmente
    :return:
    """
    tupla3 = namedtuple("tupla3", ["a1", "a2", "a3"])
    tupla2 = namedtuple("tupla2", ["b1", "b2"])

    test_df = spark.createDataFrame(
        [(tupla3("a", "b", "c"), "hola", 3, [tupla2("pepe", "juan"), tupla2("pepito", "juanito")])],
        ["tupla", "nombre", "edad", "amigos"]
    )

    aplanado_df = MotorIngesta.aplana_df(test_df)


    assert(set(["a1", "a2", "a3", "nombre", "edad", "b1", "b2"]).issubset(set(aplanado_df.columns)))
    assert("tupla" not in aplanado_df.columns)
    assert("amigos" not in aplanado_df.columns)

def test_ingesta_fichero(spark):
    """
    Comprueba que la ingesta de un fichero JSON de prueba se hace correctamente. Utiliza el fichero
    JSON existente en la carpeta tests/resources
    :param spark: SparkSession inicializada localmente
    :return:
    """

    carpeta_este_fichero = str(Path(__file__).parent)
    path_test_config = carpeta_este_fichero + "/resources/test_config.json"
    path_test_data = carpeta_este_fichero + "/resources/test_data.json"

    with open(path_test_config, 'r') as f:
        config = json.load(f)

    motor_ingesta = MotorIngesta(config)

    datos_df = motor_ingesta.ingesta_fichero(path_test_data)

    assert (datos_df.count() == 1)
    assert (set(["nombre", "parentesco", "numero", "profesion"]).issubset(set(datos_df.columns)))
    assert (len(datos_df.columns) == 4)

    primera_fila = datos_df.first()
    assert (primera_fila["nombre"] == "Juan")
    assert (primera_fila["parentesco"] == "sobrino")
    assert (primera_fila["numero"] == 3)
    assert (primera_fila["profesion"] == "Ingeniero")

def test_aniade_intervalos_por_aeropuerto(spark):
    """
    Comprueba que las variables añadidas con información del vuelo inmediatamente posterior que sale del mismo
    aeropuerto están bien calculadas
    :param spark: SparkSession inicializada localmente
    :return:
    """

    test_df = spark.createDataFrame(
        [("JFK", "2023-12-25 15:35:00", "American_Airlines"),
         ("JFK", "2023-12-25 17:35:00", "Iberia")],
        ["Origin", "FlightTime", "Reporting_Airline"]
    ).withColumn("FlightTime", F.col("FlightTime").cast("timestamp"))

    expected_df = spark.createDataFrame(
        [("JFK", "2023-12-25 15:35:00", "American_Airlines", "2023-12-25 17:35:00", "Iberia", 7200),
         ("JFK", "2023-12-25 17:35:00", "Iberia", None, None, None)],
        ["Origin", "FlightTime", "Reporting_Airline", "FlightTime_next", "Airline_next", "diff_next"]
    ).withColumn("FlightTime", F.col("FlightTime").cast("timestamp")) \
     .withColumn("FlightTime_next", F.col("FlightTime_next").cast("timestamp"))

    expected_row = expected_df.first()

    result_df = aniade_intervalos_por_aeropuerto(test_df)
    actual_row = result_df.first()

    assert (expected_row["Origin"] == actual_row["Origin"])
    assert (expected_row["FlightTime"] == actual_row["FlightTime"])
    assert (expected_row["Reporting_Airline"] == actual_row["Reporting_Airline"])
    assert (expected_row["FlightTime_next"] == actual_row["FlightTime_next"])
    assert (expected_row["Airline_next"] == actual_row["Airline_next"])
    assert (expected_row["diff_next"] == actual_row["diff_next"])



def test_aniade_hora_utc(spark):
    """
    Comprueba que la columna FlightTime en la zona horaria UTC está correctamente calculada
    :param spark: SparkSession inicializada localmente
    :return:
    """

    fichero_timezones = str(Path(__file__).parent) + "../motor_ingesta/resources/timezones.csv"

    test_df = spark.createDataFrame(
        [("JFK", "2023-12-25", 1535)],
        ["Origin", "FlightDate", "DepTime"]
    )

    expected_df = spark.createDataFrame(
        [("JFK", "2023-12-25", 1535,"2023-12-25 15:35:00")],
        ["Origin", "FlightDate", "DepTime", "FlightTimeLocal"]
    ).withColumn("FlightTime",
                 F.to_utc_timestamp(F.col("FlightTimeLocal").cast("timestamp"), F.lit("America/New_York"))
                 ).select("Origin", "FlightDate", "DepTime", "FlightTime")

    expected_row = expected_df.first()

    result_df = aniade_hora_utc(spark, test_df)
    actual_row = result_df.first()

    assert(actual_row["Origin"] == expected_row["Origin"])
    assert(actual_row["FlightDate"] == expected_row["FlightDate"])
    assert(actual_row["DepTime"] == expected_row["DepTime"])
    assert(actual_row["FlightTime"] == expected_row["FlightTime"])
