# declare spark dataframe schema

ArraySchema = StructType(
[
StructField("name", StringType(), True),
StructField("id", StringType(), True),
StructField("course", StringType(), True),
StructField("group", StringType(), True)
])

Schema = StructType(
[
StructField("school", StringType(), True),
StructField("department", StringType(), True),
StructField("studentdetails",ArrayType(ArraySchema),True)
])
