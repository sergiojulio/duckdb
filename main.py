import duckdb

# cursor = duckdb.connect()

# print(cursor.execute("SELECT * FROM 'userdata1.parquet' LIMIT 10;").fetchall())


def get_data(year, month):
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    print("processing", url, "...")

    con = duckdb.connect(database=":memory:")
    con.execute("install httpfs")  # TODO: bake into the image
    con.execute("load httpfs")
    q = """
    with sub as (
        select tpep_pickup_datetime::date d, count(1) c
        from read_parquet(?)
        group by 1
    )
    select d, c from sub
    where date_part('year', d) = ?  -- filter out garbage
    and date_part('month', d) = ?   -- same
    """
    con.execute(q, (url, year, month))
    return list(con.fetchall())

print(get_data(2022, 10))