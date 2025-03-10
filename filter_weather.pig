weather_data = LOAD '/user/root/input/seattle-weather.csv' 
                 USING PigStorage(',') 
                 AS (date:chararray, precipitation:float, temp_max:float, temp_min:float, wind:float, weather:chararray);

-- Filter records where precipitation is greater than 1.0
rainy_days = FILTER weather_data BY precipitation > 1.0;

-- Store the filtered data into HDFS output directory
STORE rainy_days INTO '/user/root/output/rainy_days' USING PigStorage(',');

-- View output in the console
-- DUMP rainy_days; 

