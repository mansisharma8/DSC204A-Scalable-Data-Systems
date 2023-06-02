import os
import pyspark.sql.functions as F
import pyspark.sql.types as T

from utilities import SEED
# import any other dependencies you want, but make sure only to use the ones
# availiable on AWS EMR

# ---------------- choose input format, dataframe or rdd ----------------------
INPUT_FORMAT = 'dataframe'  # change to 'rdd' if you wish to use rdd inputs
# -----------------------------------------------------------------------------
if INPUT_FORMAT == 'dataframe':
    import pyspark.ml as M
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    from pyspark.ml.feature import Imputer
    from pyspark.sql.functions import col, avg, variance, count, when
    from pyspark.ml.regression import DecisionTreeRegressor
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.feature import StringIndexer, OneHotEncoder, PCA
    from pyspark.ml.stat import Summarizer
if INPUT_FORMAT == 'koalas':
    import databricks.koalas as ks
elif INPUT_FORMAT == 'rdd':
    import pyspark.mllib as M
    from pyspark.mllib.feature import Word2Vec
    from pyspark.mllib.linalg import Vectors
    from pyspark.mllib.linalg.distributed import RowMatrix
    from pyspark.mllib.tree import DecisionTree
    from pyspark.mllib.regression import LabeledPoint
    from pyspark.mllib.linalg import DenseVector
    from pyspark.mllib.evaluation import RegressionMetrics


# ---------- Begin definition of helper functions, if you need any ------------

# def task_1_helper():
#   pass

# -----------------------------------------------------------------------------


def task_1(data_io, review_data, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    overall_column = 'overall'
    # Outputs:
    mean_rating_column = 'meanRating'
    count_rating_column = 'countRating'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------


# Compute mean ratings
    mean_ratings = review_data.groupBy(asin_column).agg(F.avg(overall_column).alias(mean_rating_column))

    # Compute count ratings
    count_ratings = review_data.groupBy(asin_column).agg(F.count(overall_column).alias(count_rating_column))

    # Join data
    transformed = product_data.join(mean_ratings, on=asin_column, how="left").join(count_ratings, on=asin_column, how="left")

    # Calculate total count
    count_total = transformed.count()

    # Calculate mean meanRating, variance meanRating, and numNulls meanRating
    mean_meanRating, variance_meanRating, numNulls_meanRating = transformed.select(
        F.mean(mean_rating_column),
        F.variance(mean_rating_column),
        F.count(F.when(F.col(mean_rating_column).isNull(), mean_rating_column))
    ).first()

    # Calculate mean countRating, variance countRating, and numNulls countRating
    mean_countRating, variance_countRating, numNulls_countRating = transformed.select(
        F.mean(count_rating_column),
        F.variance(count_rating_column),
        F.count(F.when(F.col(count_rating_column).isNull(), count_rating_column))
    ).first()
    # ---------------------- Put results in res dict --------------------------
    # Calculate the values programmatically. Do not change the keys and do not
    # hard-code values in the dict. Your submission will be evaluated withVBc
    # different inputs.
    # Modify the values of the following dictionary accordingly.
    res = {
        'count_total': None,
        'mean_meanRating': None,
        'variance_meanRating': None,
        'numNulls_meanRating': None,
        'mean_countRating': None,
        'variance_countRating': None,
        'numNulls_countRating': None
    }

    
    # Create a dictionary to store results
    res = {
        'count_total': count_total,
        'mean_meanRating': mean_meanRating,
        'variance_meanRating': variance_meanRating,
        'numNulls_meanRating': numNulls_meanRating,
        'mean_countRating': mean_countRating,
        'variance_countRating': variance_countRating,
        'numNulls_countRating': numNulls_countRating
    }


    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_1')
    return res
    '''
    review_data = data_dict['review']
    product_data = data_dict['product']
    transformed = review_data.groupBy('asin').agg(F.avg('overall'), F.count('overall')).toDF('asin', 'meanRating', 'countRating')# .withColumn("countRating", replace(F.col("countRating"), 0))
    merged = product_data[['asin']].join(transformed, on='asin', how="left")
    count_total = merged.count()
    '''
    # -------------------------------------------------------------------------


def task_2(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    salesRank_column = 'salesRank'
    categories_column = 'categories'
    asin_column = 'asin'
    # Outputs:
    category_column = 'category'
    bestSalesCategory_column = 'bestSalesCategory'
    bestSalesRank_column = 'bestSalesRank'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    # Create a new column 'category' from the first non-empty value in 'categories'
    product_data_cat = product_data.withColumn(
        category_column,
        F.when((F.size(F.col(categories_column)[0]) <= 0) | (F.col(categories_column)[0][0] == ''), None).otherwise(F.col(categories_column)[0][0])
    )

    # Extract 'key' and 'value' columns from 'salesRank' array
    key = product_data.select(F.explode(F.col(salesRank_column))).select(F.col("key"))
    value = product_data.select(F.explode(F.col(salesRank_column))).select(F.col("value"))

    # Calculate the results
    count_total = product_data.count()
    mean_bestSalesRank = value.select(F.avg(F.col("value"))).head()[0]
    variance_bestSalesRank = value.select(F.variance(F.col("value"))).head()[0]
    numNulls_category = product_data_cat.filter(F.col(category_column).isNull()).count()
    countDistinct_category = product_data_cat.select(category_column).distinct().count() - 1
    numNulls_bestSalesCategory = product_data.filter(F.col(salesRank_column).isNull()).count() + product_data.filter(F.size(F.col(salesRank_column)) == 0).count()
    countDistinct_bestSalesCategory = key.distinct().count()


    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_bestSalesRank': None,
        'variance_bestSalesRank': None,
        'numNulls_category': None,
        'countDistinct_category': None,
        'numNulls_bestSalesCategory': None,
        'countDistinct_bestSalesCategory': None
    }
    # Modify res:
    res = {
        'count_total': count_total,
        'mean_bestSalesRank': mean_bestSalesRank,
        'variance_bestSalesRank': variance_bestSalesRank,
        'numNulls_category': numNulls_category,
        'countDistinct_category': countDistinct_category,
        'numNulls_bestSalesCategory': numNulls_bestSalesCategory,
        'countDistinct_bestSalesCategory': countDistinct_bestSalesCategory
    }

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_2')
    return res
    # -------------------------------------------------------------------------


def task_3(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    price_column = 'price'
    attribute = 'also_viewed'
    related_column = 'related'
    # Outputs:
    meanPriceAlsoViewed_column = 'meanPriceAlsoViewed'
    countAlsoViewed_column = 'countAlsoViewed'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
# create alias for later use in self-join
    product_data_a = product_data.alias('product_data_a')

    # expand also_viewed array into multiple rows
    product_data_a = product_data_a.select(
        product_data_a[asin_column],
        F.explode_outer(product_data_a[related_column][attribute])
    ).withColumnRenamed('col', attribute)

    # create alias for later use in self-join
    product_data_b = product_data.alias('product_data_b')

    # select just asin and price
    product_data_b = product_data_b.select(
        product_data_b[asin_column],
        product_data_b[price_column]
    ).withColumnRenamed(asin_column, attribute)

    # join expanded table with price table
    product_data_flattened = product_data_a.join(product_data_b, on=attribute, how='left')

    aggregated = product_data_flattened.groupby(product_data_flattened[asin_column]).agg(
        F.count(product_data_flattened[attribute]),
        F.avg(product_data_flattened[price_column])
    )

    # turn count == 0 into null
    aggregated = aggregated.select(
        aggregated[asin_column],
        aggregated[f'avg({price_column})'],
        F.when(F.col(f'count({attribute})') == 0, None).otherwise(F.col(f'count({attribute})')).alias(f'count({attribute})')
    )

    # final aggregations
    a1 = aggregated.agg(
        F.count(aggregated[asin_column]),
        F.mean(aggregated[f'avg({price_column})']),
        F.variance(aggregated[f'avg({price_column})']),
        F.count(aggregated[f'avg({price_column})']),
        F.mean(aggregated[f'count({attribute})']),
        F.variance(aggregated[f'count({attribute})']),
        F.count(aggregated[f'count({attribute})'])
    ).toPandas()

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanPriceAlsoViewed': None,
        'variance_meanPriceAlsoViewed': None,
        'numNulls_meanPriceAlsoViewed': None,
        'mean_countAlsoViewed': None,
        'variance_countAlsoViewed': None,
        'numNulls_countAlsoViewed': None
    }
    # Modify res:
    res['count_total'] = int(a1[f'count({asin_column})'].iloc[0])
    res['mean_meanPriceAlsoViewed'] = float(a1[f'avg(avg({price_column}))'].iloc[0])
    res['variance_meanPriceAlsoViewed'] = float(a1[f'var_samp(avg({price_column}))'].iloc[0])
    res['numNulls_meanPriceAlsoViewed'] = res['count_total'] - int(a1[f'count(avg({price_column}))'].iloc[0])
    res['mean_countAlsoViewed'] = float(a1[f'avg(count({attribute}))'].iloc[0])
    res['variance_countAlsoViewed'] = float(a1[f'var_samp(count({attribute}))'].iloc[0])
    res['numNulls_countAlsoViewed'] = res['count_total'] - int(a1[f'count(count({attribute}))'].iloc[0])




    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_3')
    return res
    # -------------------------------------------------------------------------


def task_4(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    price_column = 'price'
    title_column = 'title'
    # Outputs:
    meanImputedPrice_column = 'meanImputedPrice'
    medianImputedPrice_column = 'medianImputedPrice'
    unknownImputedTitle_column = 'unknownImputedTitle'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------

    # Impute missing values
    imputer = Imputer(inputCols=[price_column], outputCols=[medianImputedPrice_column])
    imputer.setStrategy("median")
    fit_median = imputer.fit(product_data).transform(product_data)
    imputer.setOutputCols([meanImputedPrice_column])
    imputer.setStrategy("mean")
    fit_mean = imputer.fit(product_data).transform(product_data)

    # Impute unknown values in title column
    title_df = product_data.select(col(title_column)).fillna('unknown').withColumnRenamed(title_column, unknownImputedTitle_column)

    # Calculate result metrics



    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanImputedPrice': None,
        'variance_meanImputedPrice': None,
        'numNulls_meanImputedPrice': None,
        'mean_medianImputedPrice': None,
        'variance_medianImputedPrice': None,
        'numNulls_medianImputedPrice': None,
        'numUnknowns_unknownImputedTitle': None
    }
    # Modify res:
    res['count_total'] = title_df.count()
    res['mean_meanImputedPrice'] = fit_mean.select(avg(col(meanImputedPrice_column))).head()[0]
    res['variance_meanImputedPrice'] = fit_mean.select(variance(col(meanImputedPrice_column))).head()[0]
    res['numNulls_meanImputedPrice'] = fit_mean.select(count(when(col(meanImputedPrice_column).isNull(), meanImputedPrice_column))).head()[0]
    res['mean_medianImputedPrice'] = fit_median.select(avg(col(medianImputedPrice_column))).head()[0]
    res['variance_medianImputedPrice'] = fit_median.select(variance(col(medianImputedPrice_column))).head()[0]
    res['numNulls_medianImputedPrice'] = fit_median.select(count(when(col(medianImputedPrice_column).isNull(), medianImputedPrice_column))).head()[0]
    res['numUnknowns_unknownImputedTitle'] = title_df.filter(col(unknownImputedTitle_column) == "unknown").count()

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_4')
    return res
    # -------------------------------------------------------------------------


def task_5(data_io, product_processed_data, word_0, word_1, word_2):
    # -----------------------------Column names--------------------------------
    # Inputs:
    title_column = 'title'
    # Outputs:
    titleArray_column = 'titleArray'
    titleVector_column = 'titleVector'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    array_df = product_processed_data.select(F.split(F.lower(product_processed_data['title']), ' ').alias('titleArray')) # .show()
    word2Vec = M.feature.Word2Vec(vectorSize=16, minCount=100, seed=102, numPartitions=4, inputCol="titleArray", outputCol="titleVector")
    model = word2Vec.fit(array_df)

    product_processed_data_output = model.transform(array_df)

    # Get the count of the total output
    count_total = product_processed_data_output.count()

    # Get the size of the vocabulary
    size_vocabulary = model.getVectors().count()

    # Find synonyms for word_0
    word_0_synonyms = model.findSynonymsArray(word_0, 10)

    # Find synonyms for word_1
    word_1_synonyms = model.findSynonymsArray(word_1, 10)

    # Find synonyms for word_2
    word_2_synonyms = model.findSynonymsArray(word_2, 10)

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'size_vocabulary': None,
        'word_0_synonyms': [(None, None), ],
        'word_1_synonyms': [(None, None), ],
        'word_2_synonyms': [(None, None), ]
    }
    # Modify res:
    res = {
        'count_total': count_total,
        'size_vocabulary': size_vocabulary,
        'word_0_synonyms': word_0_synonyms,
        'word_1_synonyms': word_1_synonyms,
        'word_2_synonyms': word_2_synonyms
    }



    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_5')
    return res
    '''
    res = task_5(data_io, data_dict['product_processed'], 'piano', 'rice', 'laptop')
    pa2.tests.test(res, 'task_5')
    '''
    # -------------------------------------------------------------------------


def task_6(data_io, product_processed_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    category_column = 'category'
    # Outputs:
    categoryIndex_column = 'categoryIndex'
    categoryOneHot_column = 'categoryOneHot'
    categoryPCA_column = 'categoryPCA'
    # -------------------------------------------------------------------------    

    # ---------------------- Your implementation begins------------------------
    indexer = StringIndexer(inputCol=category_column, outputCol=categoryIndex_column).fit(product_processed_data)
    y = indexer.transform(product_processed_data)
    encoder = OneHotEncoder(inputCols=[categoryIndex_column], outputCols=[categoryOneHot_column], dropLast=False)
    model = encoder.fit(y)
    y = model.transform(y)
    pca = PCA(k=15, inputCol=categoryOneHot_column, outputCol=categoryPCA_column)
    model = pca.fit(y)
    y = model.transform(y)
    count_total = y.count()
    meanVector_categoryOneHot = y.select(Summarizer.mean(F.col(categoryOneHot_column))).head()[0]
    meanVector_categoryPCA = y.select(Summarizer.mean(F.col(categoryPCA_column))).head()[0]
    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'meanVector_categoryOneHot': [None, ],
        'meanVector_categoryPCA': [None, ]
    }
    # Modify res:
    res = {
        'count_total': count_total,
        'meanVector_categoryOneHot': meanVector_categoryOneHot,
        'meanVector_categoryPCA': meanVector_categoryPCA
    }


    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_6')
    return res
    # -------------------------------------------------------------------------
    
    
def task_7(data_io, train_data, test_data):
    
    # ---------------------- Your implementation begins------------------------
    from pyspark.ml.regression import RandomForestRegressor
    from pyspark.ml.evaluation import RegressionEvaluator

    # Create a RandomForestRegressor model
    rf = RandomForestRegressor(maxDepth=5).setLabelCol("overall").setFeaturesCol("features")

    # Fit the model to the training data
    model = rf.fit(train_data)

    # Make predictions on the test data
    predictions = model.transform(test_data)

    # Evaluate the predictions using RMSE
    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="overall", metricName="rmse")
    test_rmse = evaluator.evaluate(predictions)
    
    
    
    # -------------------------------------------------------------------------
    
    
    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': None
    }
    # Modify res:
    res['test_rmse'] = test_rmse

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_7')
    return res
    # -------------------------------------------------------------------------
    
    
def task_8(data_io, train_data, test_data):
    
    # ---------------------- Your implementation begins------------------------
    rmses = []
    
    # Iterate over different values of maxDepth
    for max_depth in [5, 7, 9, 12]:
        # Split the training data into training and validation sets
        (training_data, validation_data) = train_data.randomSplit([0.75, 0.25])
        
        # Create a decision tree regressor with the specified maxDepth
        dt_regressor = M.regression.DecisionTreeRegressor(featuresCol="features", maxDepth=max_depth, labelCol='overall')
        
        # Train the model on the training data
        model = dt_regressor.fit(training_data)
        
        # Make predictions on the validation data
        predictions = model.transform(validation_data)
        
        # Evaluate the model's performance using RMSE
        evaluator = M.evaluation.RegressionEvaluator(predictionCol='prediction', labelCol='overall')
        rmse = evaluator.evaluate(predictions)
        
        # Add the RMSE score to the list
        rmses.append(rmse)
    
    
    
    # -------------------------------------------------------------------------
    
    
    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': None,
        'valid_rmse_depth_5': None,
        'valid_rmse_depth_7': None,
        'valid_rmse_depth_9': None,
        'valid_rmse_depth_12': None,
    }
    # Modify res:
    res['test_rmse'] = rmses[0]
    res['valid_rmse_depth_5'] = rmses[0]
    res['valid_rmse_depth_7'] = rmses[1]
    res['valid_rmse_depth_9'] = rmses[2]
    res['valid_rmse_depth_12'] = rmses[3]


    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_8')
    return res
    # -------------------------------------------------------------------------

