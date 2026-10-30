from pyspark import pipelines as dp


#### Dim Passenger
@dp.view
def dim_passenger_view():
  df = spark.readStream.table("uber.silver.silver_obt")\
            .select("passenger_id", "passenger_name", "passenger_email", "passenger_phone")\
            .dropDuplicates(['passenger_id'])
  return df          


dp.create_streaming_table("dim_passenger")
dp.create_auto_cdc_flow(
  target = "dim_passenger",
  source = "dim_passenger_view",
  keys = ["passenger_id"],
  sequence_by = "passenger_id",
  stored_as_scd_type = "1", 
)


#### Dim Driver
@dp.view
def dim_driver_view():
  df = spark.readStream.table("uber.silver.silver_obt")\
            .select("driver_id", "driver_name", "driver_rating", "driver_phone", "driver_license")\
            .dropDuplicates(['driver_id'])
  return df

dp.create_streaming_table("dim_driver")
dp.create_auto_cdc_flow(
  target = "dim_driver",
  source = "dim_driver_view",
  keys = ["driver_id"],
  sequence_by = "driver_id",
  stored_as_scd_type = "1", 
)


#### Dim Vehicle
@dp.view
def dim_vehicle_view():
  df = spark.readStream.table("uber.silver.silver_obt")\
              .select("vehicle_id", "vehicle_make_id", "vehicle_type_id", "vehicle_model","vehicle_color","license_plate","vehicle_make","vehicle_type")\
              .dropDuplicates(['vehicle_id']) 
  return df

dp.create_streaming_table("dim_vehicle")
dp.create_auto_cdc_flow(
  target = "dim_vehicle",
  source = "dim_vehicle_view",
  keys = ["vehicle_id"],
  sequence_by = "vehicle_id",
  stored_as_scd_type = "1", 
)            


#### Dim Payment
@dp.view
def dim_payment_view():
    df = spark.readStream.table("uber.silver.silver_obt")\
          .select("payment_method_id","payment_method","is_card","requires_auth")\
          .dropDuplicates(subset=['payment_method_id'])
    return df

dp.create_streaming_table("dim_payment")
dp.create_auto_cdc_flow(
  target = "dim_payment",
  source = "dim_payment_view",
  keys = ["payment_method_id"],
  sequence_by = "payment_method_id",
  stored_as_scd_type = 1,
)


#### Dim Booking
@dp.view
def dim_booking_view():
    df = spark.readStream.table("uber.silver.silver_obt")\
          .select("ride_id","confirmation_number","dropoff_location_id","ride_status_id","dropoff_city_id","cancellation_reason_id","dropoff_address","dropoff_latitude","dropoff_longitude","booking_timestamp","dropoff_timestamp")\
          .dropDuplicates(subset=['ride_id'])
    return df

dp.create_streaming_table("dim_booking")
dp.create_auto_cdc_flow(
  target = "dim_booking",
  source = "dim_booking_view",
  keys = ["ride_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1,
)



