import polars as pl
import time

def start_stopwatch():
        return time.time()

def stop_stopwatch(start):
    end = time.time()    
    difference = end - start
    print("Time Taken: " + str(difference))

def lazyscan():
    start = time.time()
    lazy_dataset = []
    for i in range(1,12):
        month = f"{i:02d}"
        dataset = pl.scan_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"+month+".parquet")
        #dataset = pl.scan_parquet("yellow_tripdata_2024-"+month+".parquet")
        dataset = dataset.with_columns([
            pl.col("tpep_pickup_datetime").cast(pl.Datetime("ns")),
            pl.col("tpep_dropoff_datetime").cast(pl.Datetime("ns"))
        ]
        )
        lazy_dataset.append(dataset)  
    end = time.time()    
    difference = end - start
    print("Time Taken for lazyscan.") 
    print(difference)
    return pl.concat(lazy_dataset)


def eagerscan():
    combined_dataset = []
    start = start_stopwatch()
    for i in range(1,12):
        month = f"{i:02d}"
        dataset = pl.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"+month+".parquet")

        #cast datetime as nanoseconds to avoid errors
        dataset = dataset.with_columns([
            pl.col("tpep_pickup_datetime").cast(pl.Datetime("ns")),
            pl.col("tpep_dropoff_datetime").cast(pl.Datetime("ns"))
        ]
        )
        combined_dataset.append(dataset)
        print("Finshed reading month:"+month)
    df = pl.concat(combined_dataset)
    #df.write_csv("combined_file.csv")
    stop_stopwatch()
    return combined_dataset

def calculate_most_visited_locations(combined_dataset,is_lazy = False):
    start = start_stopwatch()

    df = combined_dataset.select([
        "tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance","PULocationID", "DOLocationID","fare_amount", "total_amount"
    ])
    most_visited_location = df.group_by("DOLocationID").count().sort(("count"),descending=True)
    most_picked_location = df.group_by("PULocationID").count().sort(("count"),descending=True)
    most_common_trips = df.group_by(["PULocationID","DOLocationID"]).agg([pl.count().alias("trips")]).sort(("trips"),descending=True)

    if(is_lazy):
        print(most_visited_location.limit(5).collect())
        print(most_picked_location.limit(5).collect())
        print(most_common_trips.limit(5).collect())
        stop_stopwatch(start)
    else:
        print(most_visited_location.collect())
        print(most_picked_location.collect())
        print(most_common_trips.collect())
        stop_stopwatch(start)

    
lazy_dataset = lazyscan()
calculate_most_visited_locations(lazy_dataset,True)

combined_dataset = eagerscan()
calculate_most_visited_locations(combined_dataset,False)
#dataset = pl.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet")
#print(dataset.head)
# dataset1 = pl.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet")
# zones = pl.read_csv('taxi_zone_lookup.csv')
# df1 = dataset.join(zones,)
# airport_trips = dataset.select(pl.col('payment_type'))
# print(dataset.columns)
