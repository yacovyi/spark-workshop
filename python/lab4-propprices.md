### Lab 4: Analyzing UK Property Prices

In this lab, you will work with another real-world dataset that contains residential property sales across the UK, as reported to the Land Registry. You can download this dataset and many others from [data.gov.uk](https://data.gov.uk/dataset/land-registry-monthly-price-paid-data).

___

#### Task 1: Inspecting the Data

As always, we begin by inspecting the data, which is in the `~/data/prop-prices.csv` file. Run the following command to take a look at some of the entries:

```
head ~/data/prop-prices.csv
```

Note that this time, the CSV file does not have headers. To determine which fields are available, consult the [guidance page](https://www.gov.uk/guidance/about-the-price-paid-data).

___

#### Task 2: Importing the Data

Next, load the `prop-prices.csv` file as an `DataFrame`, and register it as a temporary table so that you can run SQL queries:

```python
columns = ['id', 'price', 'date', 'zip', 'type', 'new', 'duration', 'PAON',
           'SAON', 'street', 'locality', 'town', 'district', 'county', 'ppd',
           'status']

df = spark.read.option("inferSchema", "true").option("header", "false").csv("file:///home/ubuntu/data/prop-prices.csv")
for i, col in enumerate(df.columns):                                        
     df = df.withColumnRenamed(col, columns[i])
df.registerTempTable("properties")
df.persist()
```

___

#### Task 3: Analyzing Property Price Trends

First, let's do some basic analysis on the data. Find how many records we have per year, and print them out sorted by year.

**Solution**:

```python
spark.sql("""select   year(date), count(*)
                  from     properties
                  group by year(date)
                  order by year(date)""").collect()
```

All right, so everyone knows that properties in London are expensive. Find the average property price by county, and print the top 10 most expensive counties.

**Solution**:

```python
spark.sql("""select   county, avg(price)
                  from     properties
                  group by county
                  order by avg(price) desc
                  limit    10""").collect()
```

Is there any trend for property sales during the year? Find the average property price in Greater London month over month in 2015 and 2016, and print it out by month.

**Solution**:

```python
spark.sql("""select   year(date) as yr, month(date) as mth, avg(price)
                  from     properties
                  where    county='GREATER LONDON'
                  and      year(date) >= 2015
                  group by year(date), month(date)
                  order by year(date), month(date)""").collect()
```

Bonus: use the Python `matplotlib` module to plot the property price changes month-over-month across the entire dataset.

> The `matplotlib` module is installed in the instructor-provided appliance. However, there is no X environment, so you will not be able to view the actual plot. For your own system, follow the [installation instructions](http://matplotlib.org/users/installing.html).

**Solution**:

```python
monthPrices = spark.sql("""select   year(date), month(date), avg(price)
                                from     properties
                                group by year(date), month(date)
                                order by year(date), month(date)""").collect()
import matplotlib.pyplot as plt
values = map(lambda row: row[2], monthPrices)
plt.rcdefaults()
plt.scatter(range(0,len(monthPrices)), list(values))
plt.show()
```

___

#### Discussion

Now that you have experience in working with SQL, what are the advantages and disadvantages of using it compared to the Fluent query API?
