import pandas as pd
import sqlite3
import requests
from typing import Set

def ej_1_cargar_datos_demograficos() -> pd.DataFrame:
    url = "https://public.opendatasoft.com/explore/dataset/us-cities-demographics/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B"
    data = pd.read_csv(url, sep=';')
    data.drop(columns=['Race', 'Count', 'Number of Veterans'], inplace=True)
    data.drop_duplicates(inplace=True)
    return data

def ej_2_cargar_calidad_aire(ciudades: Set[str]) -> pd.DataFrame:
    air_quality_data = []
    for ciudad in ciudades:
        response = requests.get(f"https://api-ninjas.com/api/airquality?city={ciudad}")
        if response.status_code == 200:
            data = response.json()
            air_quality_data.append(data['concentration'])
    air_quality_df = pd.DataFrame(air_quality_data)
    return air_quality_df

def create_sqlite_db_and_load_tables(demographics_df: pd.DataFrame, air_quality_df: pd.DataFrame):
    conn = sqlite3.connect('demographics_air_quality.db')
    demographics_df.to_sql('demographics', conn, if_exists='replace', index=False)
    air_quality_df.to_sql('air_quality', conn, if_exists='replace', index=False)
    return conn

def analyze_data(conn):
    query = """
    SELECT *
    FROM demographics
    JOIN air_quality ON demographics.City = air_quality.City
    ORDER BY demographics.TotalPopulation DESC
    LIMIT 10
    """
    result = pd.read_sql_query(query, conn)
    return result