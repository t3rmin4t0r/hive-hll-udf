Approximate Queries for Hive
============================

HyperLogLog and LinearCounting UDFs have been written before, that part isn't new,but nobody seems to have built a proper work-flow composable HLL++ implementation for actual data storage.

When I was the /on-call architect/ at Zynga's SEG, I used to constantly follow DAU charts as my go-to early warning tool.

The influx of new users will start to flatten out whenever Facebook had an API outage or someone enables an experiment with bad code.

For similar funnel analysis and cohort tracking with fairly large error bars, most people need time series aggregations for distinct user information and rough intersections between user groupings.

And the lag between data ingestion and it showing up on a graph is a real problem for the people who hold the knobs for experiments.

This use-case is a far more complex work-flow than an `approx_distinct` implementation, but something @prasanthj's HLL++ library can enable, if you wrote the right UDAF/UDF combinations.

If you maintained an RDD of weekly, daily and hourly values of HLL(uid) tables, then to generate an upto the hour approximate MAU calculation would involve no scans of any user data beyond the current hour.
