# Exercise

## About the "Exercise" app

Hi Team!

Via this app you can create extractions and transformations just by creating simple YAML file. You do not need any Python, Spark or Delta Lake skills. It's a declarative, config-driven way how to create ETL/ELT jobs.

Inside [specs](src/exercise/specs) folder you can create new YAML files similar to the existing one [exercise-transactions.yaml](src/exercise/specs/exercise/exercise-transactions.yaml) and add new specs. Then you can run the job just by using the spec name.

Here, I created only the transformation logic, with reading CSV data sources only. Real app would have more sources available (JDBC, Delta Lake, Kafka...), which then needs also "connection" YAML files to create etc. This is just an example how such an app could look like. Let's say there are many things to improve. The goal of this work was to create something useful, well-written of course, but I was very limited by time to make it "perfect".

### About the data processing
Here I just read the data from the project folder. Of course this is not the real case how to store the data. The same applies to storing the final transactions output also in the project data folder.

I store everything as Delta Lake tables, to have one unified way how to store all the "lakehouse" data. As you use Databricks, I guess this is also "your" way how you store the data in your lakehouse.

## How to run the app
### 1. Install Python env
```
poetry install
```

### 2. Create data folders with two csv files
In the app root folder create `data/bronze/claims` and `data/bronze/contracts` folders and add the input csv data.

### 3. Run the transformation
```
python src/exercise/transformation.py exercise-transactions
```

## "Conceptual Questions"
### How to handle new batches
In this case I use Delta Lake merge, which means that only new data is added based on the PK. There are more ways how to merge the data, so it can be extended of course. This works for this one use case.

### Performance
What we can discuss is the performance as I do not know how exactly the data is created.

If we read still the same data again and again, this would not work due to the many duplicate API calls under the hood. So this works only when we have new data every time, with just few items, which were already processed before.

Otherwise we would need to filter the existing data out from the final dataframe first and then do the API calls later, only on top of the new data, which is not yet in the table.

If there is lot of data, calling the API is not much efficient. Then it is better to somehow pre-generate the hashes in advance and then join the final data with this "hashed" table. Or to create the hash function internally as a Scala UDF etc.

### Transformations vs CSV files
I called the csv data as "bronze", but I would expect csv data to be just really the source raw data and in the bronze layer this data would be stored as Delta Lake tables instead, so that all three layers (bronze, silver and gold) use one unified way how to store the data. Then there is no need for CSV transformation as all the transformations are reading and writing only from/to Delta Lake tables. And then only extractions need different "connectors" to CSV for example.

I would be happy to discuss anything regarding (not only) to this project during the interview!

Thank you.
