# Assignment 5: Spark Structured Streaming
In this blog post, I will talk about my experience with [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html).

## Data stream
For this assignment, we were working with a Runescape-like data stream, which simulated players buying and selling items in the game. These actions result in a report string being sent into the TCP socket (`localhost` port `9999`).
This report is structured in the following format:
```
"<Material> <Item> was sold for <Amount>gp"
```
For example:
```
"Rune Halberd was sold for 612933gp"
```

## Setup
In order to work with this data stream, we first need to read it from the TCP socket. That is quite easy to set up with the following code:
```scala
val socketDF = spark.readStream
  .format("socket")
  .option("host", "0.0.0.0")
  .option("port", 9999)
  .load()
```

Then, to analyse this data, we need to set up a writing stream, which (for now) writes this stream into memory:
```scala
val streamWriterMem = socketDF
  .writeStream
  .outputMode("append")
  .format("memory")
```
This writer will now take the data from the read stream and write it into memory by appending each new row into the existing data set in memory.

There are three output modes to choose from:
1. **Append mode (default)** will only write new data to the output.
2. **Complete mode** will rewrite the whole table into the output every time there is a trigger.
3. **Update mode** will only write rows that were changed since the last trigger.

*source: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes*

Also, there are 7 different output formats, I will only talk about a couple of them here. For more info, go to [Spark documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks).

For debugging purposes, storing the results in memory is acceptable, as there is no need to use large sets of data for that.

By using this stream writer, we can look at what kind of data the stream consists of. In this case, it is just the formatted string (mentioned above) for each "purchase" made.
To do that, we first start the stream writer:
```scala
val memoryQuery = streamWriterMem  
  .queryName("memoryDF")
  .start()
```
Then we wait for a couple of seconds to gather some data and stop the writer to avoid overflowing the memory:
```scala
memoryQuery
  .awaitTermination(2000)
  
memoryQuery
  .stop()
```
After running the writer, we can run a query with the defined query name, to look at the results:
```scala
spark.sql("select * from memoryDF").show(...)
```
```
+----------------------------------------------+
|value                                         |
+----------------------------------------------+
|                    ...                       |
|Black Defender was sold for 320208gp          |
|Black Sword was sold for 145967gp             |
|Black Mace was sold for 173134gp              |
|Rune Halberd was sold for 612933gp            |
|Black Longsword was sold for 186664gp         |
|                    ...                       |
+----------------------------------------------+
```

## Stream Processing
Now, reading the data stream is all nice and fun, but we need to do something with these data. That is why we are reading it in the first place, right?

Firstly, to get any use out of these data, we need to parse the report string and extract useful information from it. Regular expression matching is the most straight-forward way of achieving this. In this case, let's extract:
* The **Material** of the item sold (String)
* The **Item type** of the item (String)
* And the **Price** which the item was sold for (Integer)

To get these values, the following regular expression can be used:
```
"\"([A-Z].+) ([A-Z].+) was sold for (\\\\d+)\""
```
Then, by using `regexp_extract` in an SQL query, we parse the interesting values from the report strings:
```scala
val q = f"SELECT regexp_extract(value, $myregex%s, 1) AS Material, regexp_extract(value, $myregex%s, 2) AS Item, CAST(regexp_extract(value, $myregex%s, 3) AS Integer) AS Price FROM runeUpdatesDF"

val runes = spark.sql(q)
```
That results in the following structured data stream:
```
+--------+---------+------+
|Material|Item     |Price |
+--------+---------+------+
|   ...  |   ...   |  ... |
|Rune    |Hatchet  |399929|
|Dragon  |Battleaxe|479543|
|Rune    |Claws    |587194|
|Iron    |Longsword|93403 |
|Bronze  |Battleaxe|53381 |
|   ...  |   ...   |  ... |
+--------+---------+------+
```

## Writing to disk
With this new structured data stream, we can perform many different analyses. However, in order to do that, we need to gather a dataset that is bigger than a few seconds worth of the stream.
We can do that by setting the `.format` parameter of our write stream to `"parquet"`, which stores the output in a chosen directory.

After running this stream writer for a while, we can find our results in the previously chosen directory.

## Working with the collected data
Now that we have collected quite a bit of data, we can perform different analyses on it.
For example, we can make a simple SQL query to see the average prices for each material type:
```scala
spark.sql("SELECT Material, avg(Price) FROM runes GROUP BY Material").show()
```
```
+--------+------------------+
|Material|        avg(Price)|
+--------+------------------+
|   Steel| 170768.4180327869|
| Mithril| 340023.5482625483|
|    Rune|         451110.88|
|    Iron|110813.76061776062|
| Adamant| 407007.0402930403|
|  Dragon|        511056.864|
|   White| 279607.4347826087|
|   Black| 233061.4170040486|
|  Bronze| 55954.38910505836|
+--------+------------------+
```

Another thing we could look at is the total amount of items sold in each material:
```scala
spark.sql("SELECT Material, count(*) AS Count FROM runes GROUP BY Material ORDER BY Count DESC").show()
```
```
+--------+--------+
|Material| Count  |
+--------+--------+
|  Bronze|    2207|
| Adamant|    2199|
| Mithril|    2175|
|   Black|    2169|
|   White|    2160|
|   Steel|    2147|
|  Dragon|    2131|
|    Iron|    2085|
|    Rune|    2069|
+--------+--------+
```

Or find out how much money players spent buying different types of swords:
```scala
spark.sql("SELECT Item, sum(price) AS Price_sum FROM runes WHERE LOWER(Item) LIKE '%sword%' GROUP BY Item").show()
```
```
+----------------+----------+
|            Item| Price_sum|
+----------------+----------+
|           Sword| 233685650|
|Two-handed sword| 382616701|
|       Longsword| 296568055|
+----------------+----------+
```

## Further steps
Now, these analyses are useful for gaining insight about the overall marketplace system in the game. However, if you are a player in this game, information about the overall system is not as useful as seeing short-term trends to base your decisions on (*e.g. "Should I sell this item now? How much should I sell it for?"*).

So, how about instead of streaming every sold item, we make insights for every micro-batch and write those?

To do that, we just need to add a few more operations on top of the stream we already have.

First, get the data by running our SQL query:
```scala
val runes_dash = spark.sql(q)
```
Now, to write a stream that contains aggregations, we can either:
- Choose the `"complete"` output mode. This way, the whole table would be written into output every batch.

Or

- Stick to the `"append"` output mode, but watermark the data, based on the timestamp (since appending with aggregated data requires watermarked data in spark).

I chose to go for the latter, because:
1. Rewriting the whole ever-expanding table does not sound like a good idea.
2. Having a timestamp on the data stream is always useful anyway.

So, to watermark our data, we first need to create a column that we watermark based on. In this case, I add a column that contains the current time stamp:
```scala
runes_dash = spark.sql(q)
    .withColumn("Timestamp", current_timestamp())
```
Then we watermark the data, based on this column with
```scala
    .withWatermark("Timestamp", "5 seconds")
```
*(I arbitrarily chose a delay threshold of 5 seconds)*

Now, this is where the magic happens:

I group the data, based on **material** and **item type** (and timestamp, of course) and calculate the average price of each combination as well as the quantity of each item sold for each micro-batch:
```scala
    .groupBy("Timestamp", "Material", "Item").agg(mean("Price").alias("Avg_price"), count("Timestamp").alias("Amount_sold"))
```
By running a writer for this stream, we get the following results:
```
+-------------------+--------+----------------+------------------+-----------+
|Timestamp          |Material|Item            |Avg_price         |Amount_sold|
+-------------------+--------+----------------+------------------+-----------+
|...                |...     |...             |...               |...        |
|2020-05-26 13:42:50|Iron    |Spear           |133350.16666666666|6          |
|2020-05-26 13:42:50|Bronze  |Hatchet         |49789.5           |2          |
|2020-05-26 13:42:50|Mithril |Hasta           |419849.0          |2          |
|2020-05-26 13:42:50|Steel   |Mace            |129985.0          |4          |
|2020-05-26 13:42:50|Black   |Halberd         |306674.85714285716|7          |
|2020-05-26 13:42:50|Bronze  |Longsword       |46718.0           |5          |
|...                |...     |...             |...               |...        |
+-------------------+--------+----------------+------------------+-----------+
```
*You can change the period of these insights (e.g. 10 seconds, 5 minutes, 24 hours, etc.) by choosing a trigger processing time yourself*
We can tweak the query to quickly and easily access any kind of insight, if we store some of the data in memory. For example:
- Seeing which items are being sold the most:
```scala
spark.sql("SELECT * FROM dashDF ORDER BY Timestamp DESC, Amount_sold DESC").show(false) 
```
```
+-------------------+--------+----------------+------------------+-----------+
|Timestamp          |Material|Item            |Avg_price         |Amount_sold|
+-------------------+--------+----------------+------------------+-----------+
|2020-05-26 13:42:50|Iron    |Hasta           |139958.18181818182|11         |
|2020-05-26 13:42:50|Mithril |Pickaxe         |380002.36363636365|11         |
|2020-05-26 13:42:50|Black   |Defender        |319841.45454545453|11         |
|2020-05-26 13:42:50|Iron    |Longsword       |93170.09090909091 |11         |
|2020-05-26 13:42:50|Steel   |Dagger          |99840.33333333333 |9          |
|2020-05-26 13:42:50|Mithril |Battleaxe       |320147.1111111111 |9          |
|2020-05-26 13:42:50|Iron    |Battleaxe       |106758.88888888889|9          |
|2020-05-26 13:42:50|Rune    |Defender        |640399.875        |8          |
|2020-05-26 13:42:50|Black   |Dagger          |133309.25         |8          |
|...                |...     |...             |...               |...        |
+-------------------+--------+----------------+------------------+-----------+
```

- Checking the most valuable items at the moment:
```scala
spark.sql("SELECT * FROM dashDF ORDER BY Timestamp DESC, Avg_price DESC").show(false)
```
```
+-------------------+--------+----------------+-----------------+-----------+
|Timestamp          |Material|Item            |Avg_price        |Amount_sold|
+-------------------+--------+----------------+-----------------+-----------+
|2020-05-26 13:42:50|Dragon  |Defender        |720132.6         |5          |
|2020-05-26 13:42:50|Dragon  |Halberd         |689048.75        |4          |
|2020-05-26 13:42:50|Dragon  |Claws           |659966.0         |4          |
|2020-05-26 13:42:50|Rune    |Defender        |640399.875       |8          |
|2020-05-26 13:42:50|Dragon  |Hasta           |629678.0         |2          |
|...                |...     |...             |...              |...        |
+-------------------+--------+----------------+-----------------+-----------+
```

It is also possible to run various aggregations with these insight tables (e.g. finding out how many "Rune" items have been sold in the chosen period, etc.), but I will not demonstrate it here, as they are trivial.

If you are **really** interested in the Runescape stock market, you can even write this stream into disk and plot a graph that shows which items are gaining or losing popularity or value. That would not show interesting results in this case, as the prices and counts are randomly generated, but in a real-world scenario, you might see some cool trends :).

---
*Sorry for a long post, hope it was not too boring to read :P.*

*Have a nice day!*
