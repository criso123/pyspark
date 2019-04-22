"""from pyspark import SparkContext
from operator import add

sc = SparkContext()

data = sc.parallelize(list("Hello World"))
counts = data.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).collect()

for (word, count) in counts:
    print("{}: {}".format(word, count))

sc.stop()
"""
#--------------------------------READING AND SAVING FILES------------------------------
"""
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

data = sc.parallelize([('Amber', 22), ('Alfred', 23), ('Skye', 4), ('Alfred', 12),
                        ('Amber', 9)])

rdd1 = sc.parallelize([('a', 1), ('b', 4), ('c', 10)])
rdd2 = sc.parallelize([('a', 4), ('a', 1), ('b', 6), ('d', 15)])
rdd3 = rdd1.leftOuterJoin(rdd2)

dataFromFile = sc.textFile('/home/adminquark/Descargas/VS14MORT.txt.gz', 4)

print(dataFromFile.take(1))

print(rdd3.collect())
rdd4 = rdd1.join(rdd2)
print(rdd4.collect())
rdd5 = rdd1.intersection(rdd2)
print(rdd5.collect())

rdd1 = rdd1.repartition(4)
print(len(rdd1.glom().collect()))

print(rdd1.map(lambda row: row[1]).reduce(lambda x, y: x + y))

dataReduce = sc.parallelize([1, 2, .5, .1, 5, .2], 1)
works = dataReduce.reduce(lambda x, y: x / y)

dataKey = sc.parallelize([('a', 4), ('b', 3), ('c', 2), ('a', 8), ('d', 2), ('b', 1),
                          ('d', 3)], 4)
print(dataKey.reduceByKey (lambda x, y: x + y).collect())
print(dataKey.countByKey().items())

#dataKey.saveAsTextFile('/home/adminquark/Descargas/data_key.txt')

def parseInput(row):
    import re
    pattern = re.compile(r'\(\'([a-z])\', ([0-9])\)')
    row_split = pattern.split(row)
    return (row_split[1], int(row_split[2]))

dataKeyReread = sc.textFile('/home/adminquark/Descargas/data_key.txt').map(parseInput)

print("Rereaded: ")
print(dataKeyReread.collect())
"""
#-------------------------------QUERIES AND SQL QUERIES-------------------------------
"""
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession.builder.appName("App v1").getOrCreate()
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

stringCSVRDD = sc.parallelize([(123, 'Katie', 19, 'brown'),
                               (234, 'Michael', 22, 'green'),
                               (345, 'Simeone', 23, 'blue')])

schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("age", LongType(), True),
    StructField("eyeColor", StringType(), True)
])

swimmers = spark.createDataFrame(stringCSVRDD, schema)
swimmers.createOrReplaceTempView("swimmers")
swimmers.printSchema()

# First option to get id where age = 22
swimmers.select("id", "age").filter("age = 22").show()
# Second option to get id where age = 22
swimmers.select(swimmers.id, swimmers.age).filter(swimmers.age == 22).show()

swimmers.select("name", "eyeColor").filter("eyeColor like 'b%'").show()

# Spark SQL
spark.sql("SELECT count(1) FROM swimmers").show()
spark.sql("SELECT id, age FROM swimmers WHERE age=22").show()
spark.sql("SELECT name, eyeColor FROM swimmers WHERE eyeColor like '%b'").show()

flightPerfFilePath = "/home/adminquark/Descargas/departuredelays.csv"
airportsFilePath = "/home/adminquark/Descargas/airports-codes-na.txt"

airports = spark.read.csv(airportsFilePath, header='true', inferSchema='true', sep='\t')
airports.createOrReplaceTempView("airports")

flightPerf = spark.read.csv(flightPerfFilePath, header='true')
flightPerf.createOrReplaceTempView("FlightPerformance")

flightPerf.cache()

#Join both datasets

"""
#spark.sql("""SELECT a.City, f.origin, SUM(f.delay) as Delays FROM FlightPerformance f
#JOIN airports a ON a.IATA = f.origin WHERE a.State = 'WA' GROUP BY a.City, f.origin
#ORDER BY SUM(f.delay) desc""").show()

#-----------------------------------CLEANING DATA-------------------------------------
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn

spark = SparkSession.builder.appName("Manipuling data").getOrCreate()

df = spark.createDataFrame([
    (1, 144.5, 5.9, 33, 'M'),
    (2, 167.2, 5.4, 45, 'M'),
    (3, 124.1, 5.2, 23, 'F'),
    (4, 144.5, 5.9, 33, 'M'),
    (5, 133.2, 5.7, 54, 'F'),
    (3, 124.1, 5.2, 23, 'F'),
    (5, 129.2, 5.3, 42, 'M'),
], ['id', 'weight', 'height', 'age', 'gender'])

print('Count of rows: {0}'.format(df.count()))
print('Count distinct of rows: {0}'.format(df.distinct().count()))
"""
#-----------------------------------DROP DUPLICATES-----------------------------------
"""
df = df.dropDuplicates()

print('Count of rows ids: {0}'.format(df.count()))
print('Count distinct of ids: {0}'.format(df.select([
    c for c in df.columns if c != 'id'
]).distinct().count())
      )

df = df.dropDuplicates(subset=[
    c for c in df.columns if c != 'id'
])

df.agg(
    fn.count('id').alias('count'),
    fn.countDistinct('id').alias('distinct')
).show()

df.withColumn('new_id', fn.monotonically_increasing_id()).show()
"""
#--------------------------------------MISSING VALUES---------------------------------
"""
df_miss = spark.createDataFrame([
    (1, 143.5, 5.6, 28, 'M', 100000),
    (2, 167.2, 5.4, 45, 'M', None),
    (3, None, 5.2, None, None, None),
    (4, 144.5, 5.9, 33, 'M', None),
    (5, 133.2, 5.7, 54, 'F', None),
    (6, 124.1, 5.2, None, 'F', None),
    (7, 129.2, 5.3, 42, 'M', 76000),
], ['id', 'weight', 'height', 'age', 'gender', 'income'])

df_miss.rdd.map(
    lambda  row: (row['id'], sum([c == None for c in row]))
).collect()

df_miss.where('id == 3').show()

df_miss.agg(*[
    (1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing')
    for c in df_miss.columns
]).show()

df_miss_no_income = df_miss.select([
    c for c in df_miss.columns if c != 'income'
])

df_miss_no_income.dropna(thresh=3).show()

means = df_miss_no_income.agg(
    *[fn.mean(c).alias(c)
      for c in df_miss_no_income.columns if c != 'gender']
).toPandas().to_dict('records')[0]

means['gender'] = 'missing'

df_miss_no_income.fillna(means).show()
"""
#---------------------------------------OUTLIERS-------------------------------------
"""
df_outliers = spark.createDataFrame([
    (1, 143.5, 5.3, 28),
    (2, 154.2, 5.5, 45),
    (3, 342.3, 5.1, 99),
    (4, 144.5, 5.5, 33),
    (5, 133.2, 5.4, 54),
    (6, 124.1, 5.1, 21),
    (7, 129.2, 5.3, 42),
], ['id', 'weight', 'height', 'age'])

cols = ['weight', 'height', 'age']
bounds = {}

for col in cols:
    quantiles = df_outliers.approxQuantile(
        col, [0.25, 0.75], 0.05
    )

    IQR = quantiles[1] - quantiles[0]

    bounds[col] = [
        quantiles[0] - 1.5 * IQR,
        quantiles[1] + 1.5 * IQR
    ]

print(bounds)

outliers = df_outliers.select(*['id'] + [
    (
        (df_outliers[c] < bounds[c][0]) |
        (df_outliers[c] > bounds[c][1])
    ).alias(c + '_o') for c in cols
])

outliers.show()

df_outliers = df_outliers.join(outliers, on='id')
df_outliers.filter('weight_o').select('id', 'weight').show()
df_outliers.filter('age_o').select('id', 'age').show()
"""
"""
#------------------------------DESCRIPTIVE STATISTICS---------------------------------
import pyspark.sql.types as typ
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Understanding data").getOrCreate()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

fraud = sc.textFile('/home/adminquark/Descargas/ccFraud.csv.gz')
header = fraud.first()

fraud = fraud \
    .filter(lambda row: row != header) \
    .map(lambda row: [int(elem) for elem in row.split(',')])

fields = [
    *[
        typ.StructField(h[1:-1], typ.IntegerType(), True)
        for h in header.split(',')
    ]
]

schema = typ.StructType(fields)

fraud_df = spark.createDataFrame(fraud, schema)

#fraud_df.printSchema()

#fraud_df.groupby('gender').count().show()

numerical = ['balance', 'numTrans', 'numIntlTrans']
desc = fraud_df.describe(numerical)
#desc.show()

#fraud_df.agg({'balance': 'skewness'}).show()
fraud_df.corr('balance', 'numTrans')

n_numerical = len(numerical)

corr = []

for i in range(0, n_numerical):
    temp = [None] * i
    for j in range(i, n_numerical):
        temp.append(fraud_df.corr(numerical[i], numerical[j]))
    corr.append(temp)

#print(corr)
"""
"""
#--------------------------------MACHINE LEARNING---------------------------------------
import pyspark.sql.types as typ
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Machine Learning lib").getOrCreate()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

labels = [
    ('INFANT_ALIVE_AT_REPORT', typ.StringType()),
    ('BIRTH_YEAR', typ.IntegerType()),
    ('BIRTH_MONTH', typ.IntegerType()),
    ('BIRTH_PLACE', typ.StringType()),
    ('MOTHER_AGE_YEARS', typ.IntegerType()),
    ('MOTHER_RACE_6CODE', typ.StringType()),
    ('MOTHER_EDUCATION', typ.StringType()),
    ('FATHER_COMBINED_AGE', typ.IntegerType()),
    ('FATHER_EDUCATION', typ.StringType()),
    ('MONTH_PRECARE_RECODE', typ.StringType()),
    ('CIG_BEFORE', typ.IntegerType()),
    ('CIG_1_TRI', typ.IntegerType()),
    ('CIG_2_TRI', typ.IntegerType()),
    ('CIG_3_TRI', typ.IntegerType()),
    ('MOTHER_HEIGHT_IN', typ.IntegerType()),
    ('MOTHER_BMI_RECODE', typ.IntegerType()),
    ('MOTHER_PRE_WEIGHT', typ.IntegerType()),
    ('MOTHER_DELIVERY_WEIGHT', typ.IntegerType()),
    ('MOTHER_WEIGHT_GAIN', typ.IntegerType()),
    ('DIABETES_PRE', typ.StringType()),
    ('DIABETES_GEST', typ.StringType()),
    ('HYP_TENS_PRE', typ.StringType()),
    ('HYP_TENS_GEST', typ.StringType()),
    ('PREV_BIRTH_PRETERM', typ.StringType()),
    ('NO_RISK', typ.StringType()),
    ('NO_INFECTIONS_REPORTED', typ.StringType()),
    ('LABOR_IND', typ.StringType()),
    ('LABOR_AUGM', typ.StringType()),
    ('STEROIDS', typ.StringType()),
    ('ANTIBIOTICS', typ.StringType()),
    ('ANESTHESIA', typ.StringType()),
    ('DELIV_METHOD_RECODE_COMB', typ.StringType()),
    ('ATTENDANT_BIRTH', typ.StringType()),
    ('APGAR_5', typ.IntegerType()),
    ('APGAR_5_RECODE', typ.StringType()),
    ('APGAR_10', typ.IntegerType()),
    ('APGAR_10_RECODE', typ.StringType()),
    ('INFANT_SEX', typ.StringType()),
    ('OBSTETRIC_GESTATION_WEEKS', typ.IntegerType()),
    ('INFANT_WEIGHT_GRAMS', typ.IntegerType()),
    ('INFANT_ASSIST_VENTI', typ.StringType()),
    ('INFANT_ASSIST_VENTI_6HRS', typ.StringType()),
    ('INFANT_NICU_ADMISSION', typ.StringType()),
    ('INFANT_SURFACANT', typ.StringType()),
    ('INFANT_ANTIBIOTICS', typ.StringType()),
    ('INFANT_SEIZURES', typ.StringType()),
    ('INFANT_NO_ABNORMALITIES', typ.StringType()),
    ('INFANT_ANCEPHALY', typ.StringType()),
    ('INFANT_MENINGOMYELOCELE', typ.StringType()),
    ('INFANT_LIMB_REDUCTION', typ.StringType()),
    ('INFANT_DOWN_SYNDROME', typ.StringType()),
    ('INFANT_SUSPECTED_CHROMOSOMAL_DISORDER', typ.StringType()),
    ('INFANT_NO_CONGENITAL_ANOMALIES_CHECKED', typ.StringType()),
    ('INFANT_BREASTFED', typ.StringType())
]

schema = typ.StructType([
    typ.StructField(e[0], e[1], False) for e in labels
])

births = spark.read.csv('/home/adminquark/Descargas/births_train.csv.gz',
                        header=True, schema=schema)

recode_dictionary = {
    'YNU': {
        'Y': 1,
        'N': 0,
        'U': 0
    }
}

selected_features = [
    'INFANT_ALIVE_AT_REPORT',
    'BIRTH_PLACE',
    'MOTHER_AGE_YEARS',
    'FATHER_COMBINED_AGE',
    'CIG_BEFORE',
    'CIG_1_TRI',
    'CIG_2_TRI',
    'CIG_3_TRI',
    'MOTHER_HEIGHT_IN',
    'MOTHER_PRE_WEIGHT',
    'MOTHER_DELIVERY_WEIGHT',
    'MOTHER_WEIGHT_GAIN',
    'DIABETES_PRE',
    'DIABETES_GEST',
    'HYP_TENS_PRE',
    'HYP_TENS_GEST',
    'PREV_BIRTH_PRETERM'
]
births_trimmed = births.select(selected_features)

import pyspark.sql.functions as func
def recode (col, key):
    return recode_dictionary[key][col]
def correct_cig(feat):
    return func \
        .when(func.col(feat) != 99, func.col(feat))\
        .otherwise(0)
rec_integer = func.udf(recode, typ.IntegerType())

births_transformed = births_trimmed \
    .withColumn('CIG_BEFORE', correct_cig('CIG_BEFORE'))\
    .withColumn('CIG_1_TRI', correct_cig('CIG_1_TRI'))\
    .withColumn('CIG_2_TRI', correct_cig('CIG_2_TRI'))\
    .withColumn('CIG_3_TRI', correct_cig('CIG_3_TRI'))

cols = [(col.name, col.dataType) for col in births_trimmed.schema]
YNU_cols = []

for i, s in enumerate(cols):
    if s[1] == typ.StringType():
        dis = births.select(s[0]).distinct().rdd.map(lambda row: row[0]).collect()
        if 'Y' in dis:
            YNU_cols.append(s[0])

births.select([
    'INFANT_NICU_ADMISSION',
    rec_integer('INFANT_NICU_ADMISSION', func.lit('YNU')
                )\
        .alias('INFANT_NICU_ADMISSION_RECODE')]).take(5)

exprs_YNU = [
    rec_integer(x, func.lit('YNU')).alias(x)
    if x in YNU_cols
    else x
    for x in births_transformed.columns
]
births_transformed = births_transformed.select(exprs_YNU)
births_transformed.select(YNU_cols[-5:]).show(5)

import pyspark.mllib.stat as st
import numpy as np

numeric_cols = ['MOTHER_AGE_YEARS', 'FATHER_COMBINED_AGE', 'CIG_BEFORE', 'CIG_1_TRI',
                'CIG_2_TRI', 'CIG_3_TRI', 'MOTHER_HEIGHT_IN', 'MOTHER_PRE_WEIGHT',
                'MOTHER_DELIVERY_WEIGHT', 'MOTHER_WEIGHT_GAIN']
numeric_rdd = births_transformed\
    .select(numeric_cols).rdd.map(lambda row: [e for e in row])
mllib_stats = st.Statistics.colStats(numeric_rdd)

for col, m, v in zip(numeric_cols, mllib_stats.mean(), mllib_stats.variance()):
    print('{0}: \t{1:.2f} \t {2:.2f}'.format(col, m, np.sqrt(v)))

categorical_cols = [e for e in births_transformed.columns if e not in numeric_cols]
categorical_rdd = births_transformed.select(categorical_cols).rdd\
    .map(lambda row: [e for e in row])

for i, col in enumerate(categorical_cols):
    agg = categorical_rdd.groupBy(lambda row: row[i])\
        .map(lambda row: (row[0], len(row[1])))
    print(col, sorted(agg.collect(), key=lambda el: el[1], reverse=True))

corrs = st.Statistics.corr(numeric_rdd)
for i, el in enumerate(corrs > 0.5):
    correlated = [
        (numeric_cols[j], corrs[i][j])
        for j, e in enumerate(el)
        if e == 1.0 and j != i]
    if len(correlated) > 0:
        for e in correlated:
            print('{0}-to-{1}: {2:.2f}'\
                  .format(numeric_cols[i], e[0], e[1]))

features_to_keep = [
    'INFANT_ALIVE_AT_REPORT',
    'BIRTH_PLACE',
    'MOTHER_AGE_YEARS',
    'FATHER_COMBINED_AGE',
    'CIG_1_TRI',
    'MOTHER_HEIGHT_IN',
    'MOTHER_PRE_WEIGHT',
    'DIABETES_PRE',
    'DIABETES_GEST',
    'HYP_TENS_PRE',
    'HYP_TENS_GEST',
    'PREV_BIRTH_PRETERM'
]
births_transformed = births_transformed.select([e for e in features_to_keep])

import pyspark.mllib.linalg as ln

for cat in categorical_cols[1:]:
    agg = births_transformed \
        .groupby('INFANT_ALIVE_AT_REPORT') \
        .pivot(cat) \
        .count()

    agg_rdd = agg \
        .rdd \
        .map(lambda row: (row[1:])) \
        .flatMap(lambda row:
                 [0 if e == None else e for e in row]) \
        .collect()

    row_length = len(agg.collect()[0]) - 1
    agg = ln.Matrices.dense(row_length, 2, agg_rdd)

    test = st.Statistics.chiSqTest(agg)
    print(cat, round(test.pValue, 4))

#--------CREATING THE FINAL DATASET-----------
import pyspark.mllib.feature as ft
import pyspark.mllib.regression as reg
import pyspark.mllib.linalg as ln

hashing = ft.HashingTF(7)
births_hashed = births_transformed.rdd.map(lambda row: [
    list(hashing.transform(row[1]).toArray())
        if col == 'BIRTH_PLACE'
        else  row[i]
        for i, col in enumerate(features_to_keep)])\
    .map(lambda row: [[e] if type(e) == int else e for e in row])\
    .map(lambda row: [item for sublist in row for item in sublist])\
    .map(lambda row: reg.LabeledPoint(row[0], ln.Vectors.dense(row[1:])))





births_train, births_test = births_hashed.randomSplit([0.6, 0.4])

from pyspark.mllib.classification import LogisticRegressionWithLBFGS

LR_Model = LogisticRegressionWithLBFGS.train(births_train, iterations=10)
LR_Results = (
    births_test.map(lambda row: row.label).zip(LR_Model.predict(births_test\
                                                                .map(lambda row: row.features)))
).map(lambda row: (row[0], row[1] * 1.0))

import pyspark.mllib.evaluation as ev

LR_evaluation = ev.BinaryClassificationMetrics(LR_Results)

print('Area under PR: {0:.2f}'.format(LR_evaluation.areaUnderPR))
print('Area under ROC: {0:.2f}'.format(LR_evaluation.areaUnderROC))

LR_evaluation.unpersist()

"""
"""
import pyspark.sql.types as typ
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.ml.feature as ft
import pyspark.ml.classification as cl
from pyspark.ml import Pipeline
import pyspark.ml.evaluation as ev

spark = SparkSession.builder.appName("Machine Learning lib").getOrCreate()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

labels = [
('INFANT_ALIVE_AT_REPORT', typ.IntegerType()),
    ('BIRTH_PLACE', typ.StringType()),
    ('MOTHER_AGE_YEARS', typ.IntegerType()),
    ('FATHER_COMBINED_AGE', typ.IntegerType()),
    ('CIG_BEFORE', typ.IntegerType()),
    ('CIG_1_TRI', typ.IntegerType()),
    ('CIG_2_TRI', typ.IntegerType()),
    ('CIG_3_TRI', typ.IntegerType()),
    ('MOTHER_HEIGHT_IN', typ.IntegerType()),
    ('MOTHER_PRE_WEIGHT', typ.IntegerType()),
    ('MOTHER_DELIVERY_WEIGHT', typ.IntegerType()),
    ('MOTHER_WEIGHT_GAIN', typ.IntegerType()),
    ('DIABETES_PRE', typ.IntegerType()),
    ('DIABETES_GEST', typ.IntegerType()),
    ('HYP_TENS_PRE', typ.IntegerType()),
    ('HYP_TENS_GEST', typ.IntegerType()),
    ('PREV_BIRTH_PRETERM', typ.IntegerType())
]

schema = typ.StructType([
    typ.StructField(e[0], e[1], False) for e in labels
])

births = spark.read.csv('/home/adminquark/Descargas/births_transformed.csv.gz',
                        header=True,
                        schema=schema)

births = births \
    .withColumn('BIRTH_PLACE_INT', births['BIRTH_PLACE']\
                .cast(typ.IntegerType()))

encoder = ft.OneHotEncoder(
    inputCol='BIRTH_PLACE_INT',
    outputCol='BIRTH_PLACE_VEC'
)

featuresCreator = ft.VectorAssembler(
    inputCols=[
        col[0]
        for col in labels[2:]] + [encoder.getOutputCol()], outputCol='features'
)

logistic = cl.LogisticRegression(
    maxIter=10,
    regParam=0.01,
    labelCol='INFANT_ALIVE_AT_REPORT'
)

pipeline = Pipeline(stages=[
    encoder,
    featuresCreator,
    logistic
])

births_train, births_test = births.randomSplit([0.7, 0.3], seed=666)
train, test, val = births.randomSplit([0.7, 0.2, 0.1], seed=666)

model = pipeline.fit(births_train)
test_model = model.transform(births_test)

print(test_model.take(1))

evaluator = ev.BinaryClassificationEvaluator(
    rawPredictionCol='probability',
    labelCol='INFANT_ALIVE_AT_REPORT'
)

print(evaluator.evaluate(test_model, {evaluator.metricName: 'areaUnderROC'}))
print(evaluator.evaluate(test_model, {evaluator.metricName: 'areaUnderPR'}))


import pyspark.ml.tuning as tune

logistic = cl.LogisticRegression(labelCol='INFANT_ALIVE_AT_REPORT')
grid = tune.ParamGridBuilder() \
    .addGrid(logistic.maxIter, [2, 10, 50]) \
    .addGrid(logistic.regParam,[0.01, 0.05, 0.3]) \
    .build()

evaluator = ev.BinaryClassificationEvaluator(
    rawPredictionCol='probability',
    labelCol='INFANT_ALIVE_AT_REPORT'
)

cv = tune.CrossValidator(
    estimator=logistic,
    estimatorParamMaps=grid,
    evaluator=evaluator
)

pipeline = Pipeline(stages=[encoder, featuresCreator])
data_transformer = pipeline.fit(births_train)

cvModel = cv.fit(data_transformer.transform(births_train))

data_train = data_transformer.transform(births_test)
results = cvModel.transform(data_train)

print(evaluator.evaluate(results, {evaluator.metricName: 'areaUnderROC'}))
print(evaluator.evaluate(results, {evaluator.metricName: 'areaUnderPR'}))

results = [
    (
        [
            {key.name: paramValue}
            for key, paramValue in zip(params.keys(), params.values())
        ], metric
    )
    for params, metric in zip(
        cvModel.getEstimatorParamMaps(),
        cvModel.avgMetrics
    )
]
print(sorted(results,
       key=lambda el: el[1],
       reverse=True)[0])

selector = ft.ChiSqSelector(
    numTopFeatures=5,
    featuresCol=featuresCreator.getOutputCol(),
    outputCol='selectedFeatures',
    labelCol='INFANT_ALIVE_AT_REPORT'
)

logistic = cl.LogisticRegression(
    labelCol='INFANT_ALIVE_AT_REPORT',
    featuresCol='selectedFeatures'
)

pipeline = Pipeline(stages=[encoder, featuresCreator, selector])
data_transformer = pipeline.fit(births_train)

tvs = tune.TrainValidationSplit(
    estimator=logistic,
    estimatorParamMaps=grid,
    evaluator=evaluator
)

tvsModel = tvs.fit(
    data_transformer.transform(births_train)
)

data_train = data_transformer.transform(births_test)
results = tvsModel.transform(data_train)

print(evaluator.evaluate(results, {evaluator.metricName: 'areaUnderROC'}))
print(evaluator.evaluate(results, {evaluator.metricName: 'areaUnderPR'}))

#classification
import pyspark.sql.functions as func

births = births.withColumn(
    'INFANT_ALIVE_AT_REPORT',
    func.col('INFANT_ALIVE_AT_REPORT').cast(typ.DoubleType())
)
births_train, births_test = births.randomSplit([0.7, 0.3])

classifier = cl.RandomForestClassifier(
    numTrees=5,
    maxDepth=5,
    labelCol='INFANT_ALIVE_AT_REPORT'
)
pipeline = Pipeline(stages=[encoder, featuresCreator, classifier])
model = pipeline.fit(births_train)
test = model.transform(births_test)

evaluator = ev.BinaryClassificationEvaluator(labelCol='INFANT_ALIVE_AT_REPORT')

print(evaluator.evaluate(test, {evaluator.metricName: 'areaUnderROC'}))
print(evaluator.evaluate(test, {evaluator.metricName: 'areaUnderPR'}))

classifier = cl.DecisionTreeClassifier(
    maxDepth=5,
    labelCol='INFANT_ALIVE_AT_REPORT'
)
pipeline = Pipeline(stages=[encoder, featuresCreator, classifier])
model = pipeline.fit(births_train)
test = model.transform(births_test)

evaluator = ev.BinaryClassificationEvaluator(labelCol='INFANT_ALIVE_AT_REPORT')

print(evaluator.evaluate(test, {evaluator.metricName: 'areaUnderROC'}))
print(evaluator.evaluate(test, {evaluator.metricName: 'areaUnderPR'}))

import pyspark.ml.clustering as clus

kmeans = clus.KMeans(
    k=5,
    featuresCol='features'
)
pipeline = Pipeline(stages=[encoder, featuresCreator, classifier])
model = pipeline.fit(births_train)
test = model.transform(births_test)
print(test.groupBy('prediction').agg({
    '*': 'count',
    'MOTHER_HEIGHT_IN': 'avg'
}).collect())

features = ['MOTHER_AGE_YEARS', 'MOTHER_HEIGHT_IN', 'MOTHER_PRE_WEIGHT',
            'DIABETES_PRE', 'DIABETES_GEST', 'HYP_TENS_PRE', 'HYP_TENS_GEST',
            'PREV_BIRTH_PRETERM', 'CIG_BEFORE', 'CIG_1_TRI', 'CIG_2_TRI', 'CIG_3_TRI'
]

featuresCreator = ft.VectorAssembler(
    inputCols=[col for col in features[1:]],
    outputCol='features'
)
selector = ft.ChiSqSelector(
    numTopFeatures=6,
    outputCol="selectedFeatures",
    labelCol='MOTHER_WEIGHT_GAIN'
)

import pyspark.ml.regression as reg

regressor = reg.GBTRegressor(
    maxIter=15,
    maxDepth=3,
    labelCol='MOTHER_WEIGHT_GAIN'
)
pipeline = Pipeline(stages=[
    featuresCreator,
    selector,
    regressor
])
weightGain = pipeline.fit(births_train)
evaluator = ev.RegressionEvaluator(
    predictionCol="prediction",
    labelCol='MOTHER_WEIGHT_GAIN'
)

print(evaluator.evaluate(weightGain.transform(births_test), {evaluator.metricName: 'r2'}))
"""
#-----------------------------------STREAMING-----------------------------------------
"""
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
lines = spark.readStream.format('socket').option('host', 'localhost')\
    .option('port', 9999).load()
words = lines.select(explode(split(lines.value, ' ')).alias('word'))
wordCounts = words.groupBy('word').count()

query = wordCounts.writeStream.outputMode('complete').format('console').start()
query.awaitTermination()