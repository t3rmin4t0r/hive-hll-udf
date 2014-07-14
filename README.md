Approximate Queries for Hive
============================

HyperLogLog and LinearCounting UDFs have been written before, that part isn't new,but nobody seems to have built a proper work-flow composable HLL++ implementation for actual data storage in Hive tables.

When I was in Zynga's SEG, I used to constantly follow DAU charts as my go-to early warning tool.

The influx of new users will start to flatten out whenever Facebook had an API outage or someone enables an experiment with bad code.

For similar funnel analysis and cohort tracking with fairly large error bars, most people need time series aggregations for distinct user information and rough intersections between user groupings.

And the lag between data ingestion and it showing up on a graph is a real problem for the people who hold the knobs for experiments.

This use-case is a far more complex work-flow than an `approx_distinct` implementation, but something @prasanth_j's HLL++ library can enable, if you wrote the right UDAF/UDF combinations.

If you maintained an RRD of daily and hourly values of HLL(uid) tables, then to generate an upto the hour approximate MAU calculation would involve no scans of any user data beyond the current hour.

You could start off simple with 

    select approx_distinct(uid) from raw_data;

But for the full composability example, you can see why splitting up the components of `approx_distinct` was a good idea

    days as (select hll_merge(uid_hll) as d_hll from days_rrd where day between ....),
    hours as (select hll_merge(uid_hll) h_hll as  from today_rrd where hour between ...)
      select hll_count(hll_union(days.d_hll, hours.d_hll, current.c_hll)) as mau from  
         (select hll(uid) as c_hll from       current_hour_raw) current;

You can use these with `GROUP BY ... WITH CUBE` or `ROLLUP` as well, if you have multiple groupings in the report.

You can do more set cardinality operations as well, but it multiplies the error bar everytime you operate on it - you can do `2*hll_count(hll_union(a,b)) - hll_count(a) - hll_count(b)` for approximate interesections.

The current functions are 

* CREATE TEMPORARY FUNCTION hll as 'org.notmysock.hive.UDAFHyperLogLog';
* CREATE TEMPORARY FUNCTION hll_merge as 'org.notmysock.hive.UDAFHyperLogLogMerge';
* CREATE TEMPORARY FUNCTION hll_count as 'org.notmysock.hive.UDFHyperLogLogValue';
* CREATE TEMPORARY FUNCTION hll_debug as 'org.notmysock.hive.UDFHyperLogLogDebug';
* CREATE TEMPORARY FUNCTION hll_union as 'org.notmysock.hive.UDFHyperLogLogUnion';
* CREATE TEMPORARY FUNCTION approx_distinct as 'org.notmysock.hive.UDAFApproximateDistinct';

Or well, you could add these as permanent functions into your HiveServer2.
