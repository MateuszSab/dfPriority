# Selecting the most important rows per assigned priority

Write a structured query that selects the most important rows per assigned priority.


## Input Dataset
val input = Seq(
  (1, "MV1"),
  (1, "MV2"),
  (2, "VPV"),
  (2, "Others")).toDF("id", "value")
scala> input.show
+---+------+
| id| value|
+---+------+
|  1|   MV1|
|  1|   MV2|
|  2|   VPV|
|  2|Others|
+---+------+

## Result
scala> solution.show(truncate = false)
+---+----+
| id|name|
+---+----+
|  1| MV1|
|  2| VPV|
+---+----+
