# Databricks notebook source
# MAGIC %md
# MAGIC # **Installation of Required Libraries**

# COMMAND ----------

!pip install wordcloud
!pip install nltk

# COMMAND ----------

# MAGIC %md
# MAGIC #**Importing Necessary Modules**

# COMMAND ----------

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import pandas as pd
from typing import Callable, Dict
from pyspark.sql.functions import size, split, count, col, avg, min as sp_min, max as sp_max, round as spark_round, lit, concat
from pyspark.sql import Row, DataFrame
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import numpy as np
from pyspark.ml.feature import CountVectorizer, Tokenizer, StandardScaler, StringIndexer, StopWordsRemover
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# COMMAND ----------

# Downloading NLTK packages for removing stopwords from text
nltk.download('stopwords')
nltk.download('punkt')

# Definition of stopwords list (English text -> English stopwords)
stopwords_default = list(stopwords.words('english'))

# COMMAND ----------

# MAGIC %md
# MAGIC # **EDA**

# COMMAND ----------

# MAGIC %md
# MAGIC ##*Importing Dataset*

# COMMAND ----------

!wget https://proai-datasets.s3.eu-west-3.amazonaws.com/wikipedia.csv
dataset = pd.read_csv('/databricks/driver/wikipedia.csv')
spark_df = spark.createDataFrame(dataset)
spark_df = spark_df.drop("Unnamed: 0")

# Show DataFrame
display(spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## *Data Exploration*

# COMMAND ----------

# Show unique categories
categoria_s = spark_df.select('categoria').distinct()
categoria_s.show()
print(f"Number of categories = {categoria_s.count()}")

# Show null values and statistics
print(dataset.isnull().sum())
print(dataset.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## *Data Cleaning*

# COMMAND ----------

# Checking whether null rows in 'summary' and 'documents' match
spark_df = spark_df.withColumn("both_null", col("summary").isNull() & col("documents").isNull())
both_null_df = spark_df.filter(col("both_null"))
count_both_null = both_null_df.count()
print(f"Number of rows where both 'summary' and 'documents' are null: {count_both_null}")

# Fill null values
"""
I decided to fill the null values with blanks because if I had dropped rows with null values, I would have also dropped rows belonging to the other columns and thus, alternately, useful data for the 'summary' and 'documents' column. 
However, by doing tokenization (for classification) later and splitting via 'space' (for EDA) the blanks will not be considered. 
"""
spark_df = spark_df.fillna({'summary': '', 'documents': ''})

# COMMAND ----------

# MAGIC %md
# MAGIC ## *Feature Engineering*

# COMMAND ----------

# Count the number of articles per category
article_count = spark_df.groupBy('categoria').agg(count('title').alias('num_articles'))
display(article_count)

# COMMAND ----------

# Creating a new dataframe that contains columns with the number of words in the 'summary' and 'documents' columns
df = spark_df.withColumn('num_summary_words', size(split(col('summary'), ' ')))\
             .withColumn('num_documents_words', size(split(col('documents'), ' ')))\
             .withColumn('tot_num_words', col('num_summary_words') + col('num_documents_words'))

# Aggregate statistics by category
results = df.groupBy('categoria').agg(
    count('title').alias('num_articles'),
    spark_round(avg('tot_num_words'), 2).alias('avg_num_words'),
    sp_min('tot_num_words').alias('num_words_shortest_article'),
    sp_max('tot_num_words').alias('num_words_longest_article'))
display(results)

# COMMAND ----------

# Concatenation of the columns 'summary' and 'documents'
df_cw = df.withColumn('sum_doc', concat(col('summary'), lit(' '), col('documents')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## *Word Cloud*

# COMMAND ----------

# Loop through Unique Categories
for category in df_cw.select('categoria').distinct().collect():    #retrieves unique values using distinct() and collects them into a list of rows using collect()
    # Join with a space the sum_doc values of the rows collected by filtering the DataFrame to include only those in which the category column matches the current category in the loop.
    text = " ".join(row.sum_doc for row in df_cw.filter(df_cw.categoria == category.categoria).select("sum_doc").collect())

    # Generate Word Cloud
    wordcloud = WordCloud(width=800, height=400, colormap='twilight', stopwords=stopwords_default).generate(text)
    
    # Display Word Cloud
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.title(f"Word Cloud for {category['categoria']}")
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Classification**

# COMMAND ----------

# MAGIC %md
# MAGIC ## *Useful Functions*

# COMMAND ----------

def evaluate_model(predictions: DataFrame, labelCol: str = 'target', predictionCol: str = 'prediction') -> Dict[str, float]:
    """
    Evaluate the given predictions using multiple metrics.

    Args:
        predictions (DataFrame): The predictions DataFrame.
        labelCol (str): The name of the label column.
        predictionCol (str): The name of the prediction column.

    Returns:
        Dict[str, float]: A dictionary with evaluation metrics and their respective values.
    """
    metrics = ['accuracy', 'f1']
    results = {}

    for metric in metrics:
        evaluator = MulticlassClassificationEvaluator(labelCol=labelCol, predictionCol=predictionCol, metricName=metric)
        results[metric] = np.round(evaluator.evaluate(predictions), 4)

    return results


def pipe_transf(inputCol: str, model_type) -> Pipeline:
    """
    Create a pipeline for text processing and model training.

    Args:
        inputCol (str): The name of the input column containing text.
        model_type: The type of model to be used in the pipeline.

    Returns:
        Pipeline: A Spark ML pipeline.
    """
    tokenizer = Tokenizer(inputCol=inputCol, outputCol='tokens')
    remover = StopWordsRemover(inputCol='tokens', outputCol='filtered_tokens', stopWords=stopwords_default)
    counter = CountVectorizer(inputCol='filtered_tokens', outputCol='counter_features')
    scaler = StandardScaler(inputCol='counter_features', outputCol='scaled_features')
    modeler = model_type(featuresCol='scaled_features', labelCol='target')
    pipe = Pipeline(stages=[tokenizer, remover, counter, scaler, modeler])

    return pipe


def fit_and_transform(pipe: Pipeline, train: DataFrame, test: DataFrame) -> (DataFrame, DataFrame):
    """
    Fit the pipeline to the training data and transform both training and test data.

    Args:
        pipe (Pipeline): The Spark ML pipeline.
        train (DataFrame): The training DataFrame.
        test (DataFrame): The test DataFrame.

    Returns:
        (DataFrame, DataFrame): Transformed training and test DataFrames.
    """
    model = pipe.fit(train)
    pred_train = model.transform(train)
    pred_test = model.transform(test)

    return pred_train, pred_test


def print_evaluation(pred_train: DataFrame, pred_test: DataFrame):
    """
    Print evaluation metrics for both training and test data.

    Args:
        pred_train (DataFrame): Transformed training DataFrame with predictions.
        pred_test (DataFrame): Transformed test DataFrame with predictions.
    """
    evaluation_results_train = evaluate_model(pred_train)
    print("Training Data Evaluation:")
    for metric, value in evaluation_results_train.items():
        print(f"{metric.capitalize()}: {value:.3f}")

    evaluation_results_test = evaluate_model(pred_test)
    print("\nTest Data Evaluation:")
    for metric, value in evaluation_results_test.items():
        print(f"{metric.capitalize()}: {value:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## *Data Pre-Processing*

# COMMAND ----------

# Convert Categorical String Values into numerical Indices.
indexer = StringIndexer(inputCol='categoria', outputCol='target')
df_class = indexer.fit(df_cw).transform(df_cw)

# Shows the Indexing by matching the Categories with the obtained Indexes.
target = df_class.select('categoria', 'target').distinct().orderBy('target')
display(target)

# COMMAND ----------

# Dataset division into train and test
train, test = df_class.randomSplit([0.8, 0.2], seed=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## *Model training*

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Running the pipeline for the 'summary' column*

# COMMAND ----------

pipe_sum = pipe_transf('summary', LogisticRegression)

# COMMAND ----------

pred_train_sum, pred_test_sum = fit_and_transform(pipe_sum, train, test)

# COMMAND ----------

# MAGIC %md
# MAGIC #### *Results*

# COMMAND ----------

print_evaluation(pred_train_sum, pred_test_sum)

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Running the pipeline for the 'documents' column*

# COMMAND ----------

pipe_doc = pipe_transf('documents', LogisticRegression)

# COMMAND ----------

pred_train_doc, pred_test_doc = fit_and_transform(pipe_doc, train, test)

# COMMAND ----------

# MAGIC %md
# MAGIC #### *Results*

# COMMAND ----------

print_evaluation(pred_train_doc, pred_test_doc)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Conclusions**

# COMMAND ----------

# MAGIC %md
# MAGIC The model performs better on the “Documents” column than on the “Summary” column in the test data. 
# MAGIC The higher accuracy in “Documents” could be due to the fact that the model has more data on which to base its decisions (more words and context), reducing the ambiguity that might exist in a short summary.
# MAGIC Given the relatively small discrepancy between performance on the training and test data, we can confirm that the model has good generalization ability, in both cases but especially for the “Documents” column.
# MAGIC Thus, it would be more advantageous to use the entire document for categorization whenever possible.