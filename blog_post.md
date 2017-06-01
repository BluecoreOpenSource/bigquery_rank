
## Getting Around BigQuery Sorting Limitations

#### Context

At Bluecore, our infrastucture is built on top of Google Cloud Platform, in particular, some of the transactional data we extract from the integration on our partners' websites lives in BigQuery. BigQuery is an append-only "data warehouse for analytics", which is basically a Google managed, fast, and cloud based SQL solution.   

Although BigQuery is performant for some queries (typically joins on massive tables), some simple queries fail when they involve sorting or window functions, since BigQuery is generally not happy with CPU heavy operations. Google is well aware of this limitation, however the claim is that sorting usually only matters for a small subset of records. At the Google Next conference of March 2017, it was hinted that the BigQuery team might provide a fix for this limitation in the future.  

A few months ago, we were working on a data pipeline to extract insights from our partners data, and part of the analysis involved computing percentiles for a partners' customers with respect to a simple metric. Let's take a specific example, assume that we have a 10M record table with two fields, a customer id and a historical number of purchases, and we would like to compute a table with a customer id, the historical number of purchases and the percentile with regard to the latter. The legacySQL query would look something like this:

~~~~
SELECT
   id,
   historical_num_purchases,
   NTILE(100) OVER (ORDER BY historical_num_purchases DESC) percentile
FROM
   [<table_location>]
~~~~

This query runs fine for a table with 2 columns that has up to ~8M records, for bigger tables the following error appears:  
`Error: Resources exceeded during query execution.`  
I remember the first time I encountered this error, I broke the massive SQL queries into pieces to find the bottleneck. I could not believe it was the percentile step that broke, because, well, I expected computing quantiles to be so basic it should work. 

#### First Idea

When I finally accepted the fact that percentiles break queries, it came time to think about workarounds.
A first idea was to see the problem the following way, does someone really need to have access to all the 
data to complete a task as simple as computing percentiles? A popular data science paradigm is to randomly subsample a
substantial chunk of data, derive statistics on this chunk, and interpolate to the whole population. In our use case, this means downloading a subsample of the data, computing percentile boundaries in memory and programmatically creating a SQL
query
that would define percentiles using those boundaries.

The first query would look like the following:  

~~~~
SELECT
   historical_num_purchases
FROM
   [<table_location>]
LIMIT 1000000
~~~~
NB: we are making the assumption that the above query gives a random subsample of the table which is not safe in general.  

Once we get this table in memory, we compute the boundaries of each percentile and create a query that would look like the following.   

~~~~
SELECT
   id,
   CASE
     WHEN historical_num_purchases < upper_bound_first_percentile THEN 1,
     WHEN historical_num_purchases >= lower_bound_second_percentile AND historical_num_purchases < upper_bound_second_percentile THEN 2,
     ...
     WHEN historical_num_purchases >= lower_bound_100th_percentile THEN 100 
   END approximate_percentile
FROM
   [<table_location>]
~~~~

Of course, this approach has multiple issues, the first one being edge effects that might end up giving the first and last approximate percentiles a number of samples greater than the other approximate percentiles. Also this solution only makes sense for a data distribution that only has a limited number of duplicate values. In the specific case of historical number of purchases, the distribution of values is typically close to a Poisson distribution, with the majority being duplicate values. Even though there might be fixes to these issues, those would add an unnecessary degree of complexity.

#### Better

Okay, let's back up for a second. To get quantiles all we need to do is sort the table, and then compute some sort of interpolation to assign each of the record rank to a percentile.

~~~~
SELECT
   id,
   historical_num_purchases,
   ROW_NUMBER() OVER (ORDER BY historical_num_purchases DESC) rank
FROM
   [<table_location>]
~~~~

Of course, a query like the one above would also fail with a resourceExceeded error.  

How about, instead, downloading the full dataset into memory, sorting and ranking it, and then loading it back into a BigQuery table.  A-ha! that works!  
The irony of this solution is not lost on us: a big data problem solved by simply getting a file to a single machine, sorting it using a unix command and loading it back. Here is a standalone python script to perform this operation: https://github.com/TriggerMail/bigquery_rank/
This approach might not be reasonable if the amount of data involved is substantial, in which case it would require more sophisticated methods (MapReduce/Spark?), but in our case it absolutely is.  
Specifically, when working in a Google App Engine Flexible environment (for those not familiar with this environment, think of it as a single machine), the workflow would be the following (durations shown for our specific example of 10M records with 2 fields).

- get the BQ table to a compressed csv file in Google Cloud Storage (~2 minutes)
- get the file into a flexible environment module (~1 minute)
- decompress, sort and rank the file (~2 minutes)
- load the new file to BQ (~7 minutes)

Hooray! This takes around 12 minutes for our table and the bottleneck is the transit of the data, since the sorting only
takes a couple minutes.  
At this point we have access to a new table with 3 fields: **id, historical_num_purchases, rank**, a simple query can now
compute the percentiles:

~~~~
#standardSQL
WITH MaxRank AS (
  SELECT max(rank) m
  FROM `<new_table_location>`
)
SELECT new_table.*, CEIL(100 * rank / MaxRank.m) percentile
FROM `<new_table_location>` as new_table
JOIN MaxRank
ON True
~~~~

#### Conclusion  
Of course, this ends up being a fair amount of work for a simple task, but this is a bullet worth taking considering BigQuery
strengths: scalability, performance and reliability (not on every task :)). This solution might not scale to significantly larger tables, but is a suitable one for the data size we expect in the foreseeable future.  Therefore, even though we do use Spark (through Dataproc) for heavy tasks e.g. matrix factorization, one of our core principles at Bluecore resides in avoiding unnecessarily complex solutions.  
