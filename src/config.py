"""
Configuration constants and schema definitions.
Contains physics parameters, file schemas, and reference lists for the project.
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType

# --- PHYSICS CONSTANTS ---
METRO_POPULATION_THRESHOLD = 200000 
METRO_POP_CONSTANT = -1/1443000
METRO_POWER_CONSTANT = -1.4
EARTH_RADIUS_KM = 6371.0
MIN_METRO_CITY_RADIUS = 10.0
MAX_METRO_CITY_RADIUS = 90.0

# --- DATA CLEANING ---
VALID_CITY_CODES = ['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLC', 'PPLL', 'PPLF']

# Kept for reference, though filtering logic now relies strictly on feature codes
CITIES_WITH_POWIAT_RIGHTS = [
    "Jelenia Góra", "Legnica", "Wałbrzych", "Wrocław", 
    "Bydgoszcz", "Grudziądz", "Toruń", "Włocławek", 
    "Biała Podlaska", "Chełm", "Lublin", "Zamość", 
    "Gorzów Wielkopolski", "Zielona Góra", 
    "Łódź", "Piotrków Trybunalski", "Skierniewice", 
    "Kraków", "Nowy Sącz", "Tarnów", 
    "Ostrołęka", "Płock", "Radom", "Siedlce", "Warszawa", 
    "Opole", "Krosno", "Przemyśl", "Rzeszów", "Tarnobrzeg", 
    "Białystok", "Łomża", "Suwałki", 
    "Gdańsk", "Gdynia", "Słupsk", "Sopot", 
    "Bielsko-Biała", "Bytom", "Chorzów", "Częstochowa", "Dąbrowa Górnicza", 
    "Gliwice", "Jastrzębie-Zdrój", "Jaworzno", "Katowice", "Mysłowice", 
    "Piekary Śląskie", "Ruda Śląska", "Rybnik", "Siemianowice Śląskie", 
    "Sosnowiec", "Świętochłowice", "Tychy", "Zabrze", "Żory", 
    "Kielce", "Elbląg", "Olsztyn", 
    "Kalisz", "Konin", "Leszno", "Poznań", 
    "Koszalin", "Szczecin", "Świnoujście"
]

SCHEMA = StructType([
    StructField("geonameid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("asciiname", StringType(), True),
    StructField("alternatenames", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("feature_class", StringType(), True),
    StructField("feature_code", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("cc2", StringType(), True),
    StructField("admin1_code", StringType(), True),
    StructField("admin2_code", StringType(), True),
    StructField("admin3_code", StringType(), True),
    StructField("admin4_code", StringType(), True),
    StructField("population", IntegerType(), True),
    StructField("elevation", IntegerType(), True),
    StructField("dem", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("modification_date", StringType(), True)
])