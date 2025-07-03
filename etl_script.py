from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, round, current_timestamp
import requests, psycopg2, os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

def get_api_data(url):
    try:
        response = requests.get(url)
        df = spark.read.json(spark.sparkContext.parallelize(response.json()))
        return df
    except Exception as e:
        raise(e) 
    
def postgres_conn():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('PG_DB'),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"), 
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT")
        )
        return conn
    except Exception as e:
        raise (e)

def get_postgres_options():
    return {
        "url": "jdbc:postgresql://"+os.getenv("PG_HOST")+":"+os.getenv("PG_PORT")+"/"+os.getenv('PG_DB'),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

def extract_data():
    cont_df = get_api_data('https://disease.sh/v3/covid-19/continents')

    cou_df = get_api_data('https://disease.sh/v3/covid-19/countries')

    continent_df = cont_df.select(col('updated').alias('continent_id'),'continent',
        round(col('continentInfo.lat').cast('double'),2).alias('cont_lat'),round(col('continentInfo.long').cast('double'),2).alias('cont_long')
    )

    cont_cases_df = cont_df.select(
        'continent','population','cases','deaths','recovered','active','critical','tests'
    )#cases_by_continent

    country_df = cou_df.select(col('updated').alias('country_id'),'country',col('countryInfo.iso3').alias('abreviation'),
                               round(col('countryInfo.lat').cast('double'),2).alias('count_lat'),round(col('countryInfo.long').cast('double'),2).alias('count_long'),
                               'continent')

    cou_cases_df = cou_df.select('country','population', 'cases', 'deaths', 
                                 'recovered', 'active', 'critical', 'tests')#cases_by_country
    
    dimension_df = continent_df.join(country_df, on='continent', how='inner')#location
    dimension_df = dimension_df.withColumn('insert_time',current_timestamp())

    return cont_cases_df, cou_cases_df, dimension_df

def load_data(df, table):
    options = get_postgres_options()
    df.write.format('jdbc').option("url", options["url"]) \
        .option("dbtable", table) \
        .option("user", options["user"]) \
        .option("password", options["password"]) \
        .option("driver", options["driver"]) \
        .mode("overwrite") \
        .save()
    return 

def merge_into_main():
    conn = postgres_conn()
    cursor = conn.cursor()
    cursor.execute('call MERGE_DATA()')
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Covid_19_ETL').getOrCreate()
    cont_cases_df, cou_cases_df, dimension_df = extract_data()
    load_data(dimension_df, 'location_stg')
    load_data(cou_cases_df, 'country_cases_stg')
    load_data(cont_cases_df, 'continent_cases_stg')
    merge_into_main()
    spark.stop()
    
    