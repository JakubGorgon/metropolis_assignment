from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# --- PHYSICS CONSTANTS ---
# Population at which a city becomes a "Metropolis" (an Eater)
METRO_POPULATION_THRESHOLD = 200000 

# Gravity Model Constants (From assignment)
METRO_CITY_POPULATION_CONSTANT = -1/1443000
MIN_METRO_CITY_RADIUS = 10.0
MAX_METRO_CITY_RADIUS = 100.0 - MIN_METRO_CITY_RADIUS
METRO_CITY_POWER_CONSTANT = -1.4

# --- DATA CLEANING ---
VALID_CITY_CODES = ['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLC', 'PPLL', 'PPLF']

# MIASTA NA PRAWACH POWIATU W POLSCE (https://pl.wikipedia.org/wiki/Miasto_na_prawach_powiatu)
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
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
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