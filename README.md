Approximate Queries for Hive
============================

HyperLogLog and LinearCounting UDFs have been written before, that part isn't new,but nobody seems to have built a proper work-flow composable HLL++ implementation for actual data storage in Hive tables.

When I was in Zynga's SEG, I used to constantly follow DAU charts as my go-to early warning tool.

The influx of new users will start to flatten out whenever Facebook had an API outage or someone enables an experiment with bad code.

For similar funnel analysis and cohort tracking with fairly large error bars, most people need time series aggregations for distinct user information and rough intersections between user groupings.

And the lag between data ingestion and it showing up on a graph is a real problem for the people who hold the knobs for experiments.

This use-case is a far more complex work-flow than an `approx_distinct` implementation, but something @prasanthj's HLL++ library can enable, if you wrote the right UDAF/UDF combinations.

If you maintained an RDD of daily and hourly values of HLL(uid) tables, then to generate an upto the hour approximate MAU calculation would involve no scans of any user data beyond the current hour.

For a HIVE-13 build, you could do that with

    days as (select hll\_merge(uid\_hll) as d\_hll from days_rrd where day between ....),
    hours as (select hll\_merge(uid\_hll) h\_hll as  from today_rrd where hour between ...)
      select hll\_count(hll\_union(days.d\_hll, hours.d\_hll, current.c\_hll)) as mau from  
         (select hll(uid) as c\_hll from       current\_hour\_raw) current;

For cohort analysis, you can use these with `GROUP BY ... WITH CUBE` or `ROLLUP` as well.

The current functions are 

* CREATE TEMPORARY FUNCTION hll as 'org.notmysock.hive.UDAFHyperLogLog';
* CREATE TEMPORARY FUNCTION hll_merge as 'org.notmysock.hive.UDAFHyperLogLogMerge';
* CREATE TEMPORARY FUNCTION hll_count as 'org.notmysock.hive.UDFHyperLogLogValue';
* CREATE TEMPORARY FUNCTION hll_debug as 'org.notmysock.hive.UDFHyperLogLogDebug';
* CREATE TEMPORARY FUNCTION hll_union as 'org.notmysock.hive.UDFHyperLogLogUnion';
* CREATE TEMPORARY FUNCTION approx_distinct as 'org.notmysock.hive.UDAFApproximateDistinct';

Or well, you could add these as permanent functions into your HiveServer2.
