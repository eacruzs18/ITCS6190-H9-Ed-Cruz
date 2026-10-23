# Hands-on L9: Report

**Name: Ed Cruz** 
**Student ID: 801337161** 
**Email: ecruz13@charlotte.edu** 

---

## Seed and commands

Seed used for `datagen.py`: `[801337161]`

Commands, in order (the README steps):

1. Generate data
```bash
python3 datagen.py 801337161
```
2. Start the cluster
```bash
docker compose up -d
```
3. Run `main.py`
```
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output
```
4. Stop the cluster
```bash
docker compose down
```

---

## Results

For each task, your output and one or two sentences on what it says about your data.

### Task 1: hashtag trends (DataFrame API)

```
+---------+-----+
|hashtag  |count|
+---------+-----+
|#bug     |2048 |
|#fail    |2011 |
|#design  |907  |
|#darkmode|873  |
|#ai      |837  |
|#update  |826  |
|#ux      |801  |
|#mood    |557  |
|#cleanui |303  |
|#social  |302  |
+---------+-----+
```

`#bug` and `#fail` are the top two, each with more than twice the count of `#design`. Posts are mostly about bugs and failures.

How many distinct spellings did your most frequent hashtag have before `lower` and `trim`?

Two: `#bug` (1756 times) and `#BUG` (292 times). 1756 + 292 = 2048, the same count as after cleaning.

### Task 2: engagement by age group (Spark SQL)

```
+---------+-----+---------+------------+
|age_group|posts|avg_likes|avg_retweets|
+---------+-----+---------+------------+
|Senior   |1756 |55.14    |13.37       |
|Adult    |1939 |50.78    |13.0        |
|Teen     |1305 |47.94    |12.01       |
+---------+-----+---------+------------+
```

Seniors get the most likes on average and Teens the fewest. The gap is small, about 7 likes.

### Task 3: sentiment versus engagement (Spark SQL)

```
s+---------+-----+---------+------------+
|sentiment|posts|avg_likes|avg_retweets|
+---------+-----+---------+------------+
|Negative |1018 |24.03    |6.4         |
|Neutral  |2234 |47.09    |11.7        |
|Positive |1748 |73.35    |18.14       |
+---------+-----+---------+------------+
```

Does sentiment pay on your data? Say it with the numbers.

Yes. Positive posts average 73.35 likes and 18.14 retweets. Negative posts average 24.03 likes and 6.4 retweets. That is about 3 times the likes and about 2.8 times the retweets.

### Task 4: top verified users (DataFrame API)

```
+--------------+-----------+--------------+-----+
|username      |total_likes|total_retweets|reach|
+--------------+-----------+--------------+-----+
|@meme_lord25  |20279      |5268          |25547|
|@calm_mind45  |11086      |2692          |13778|
|@rage_user3   |11023      |2417          |13440|
|@calm_mind6   |10347      |2518          |12865|
|@daily_vibes19|8275       |1788          |10063|
+--------------+-----------+--------------+-----+
```

`@meme_lord25` has the top reach, 25,547, almost double the second user (13,778).

---

## Two languages, one plan

Paste the two `explain()` outputs of task 2, the SQL version and the DataFrame version:

SQL version:

```
=== task2: physical plan of the SQL version ===
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#61 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#61 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=697]
      +- HashAggregate(keys=[age_group#10], functions=[count(post_id#0), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=694]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(post_id#0), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [post_id#0, likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [post_id#0,user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<post_id:string,user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=689]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

DataFrame version:

```
=== task2: physical plan of the DataFrame version ===
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#145 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#145 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=747]
      +- HashAggregate(keys=[age_group#10], functions=[count(post_id#0), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=744]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(post_id#0), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [post_id#0, likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [post_id#0,user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<post_id:string,user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=739]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

Your comparison: are the two plans the same? Which operator is the join and which kind of
join did Spark choose, where are the `Exchange` operators and why are they needed, and what
does this tell you about choosing between SQL and the DataFrame API?

Yes, the plans are identical. Spark chose a BroadcastHashJoin by broadcasting the users table. The Exchange operators handle broadcasting the small table, shuffling data for age-group aggregation, and range-partitioning for the final sort. Both, SQL and the DataFrame API offer same performance, so choice depends on readability only.

---

## Views

What did `createOrReplaceTempView("posts")` do, and what did it not do? Was any data copied?
What happens to the views when the program ends, and how would you keep a table for the
next session?

It gave the DataFrame `posts` a name, so SQL can use it. It did not copy any data and did not read the file again. The view is only the plan (the steps to read the CSV). When the program ends, the views are gone, because they only live in that Spark session. To keep a table for the next session, write the DataFrame to files with `df.write.parquet(...)` and read them back with `spark.read.parquet(...)`.



---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

Nothing went wrong.

