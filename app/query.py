import pyspark
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import sys
import re
import math

# get words from the text
def tokenize_text(text):
    return re.findall(r'\w+', text.lower())
print(sys.argv[1:])

# epsilon is added to avoid division by 0
def compute_BM25(total_num_of_documents, dl_avg, q_words, term_freq_dict, term_doc_freq_dict, doc_length_dict, document_id, k=1, b=0.75, epsilon=1e-6):
    result = 0
    if document_id in doc_length_dict:
        dl = doc_length_dict[document_id]
    else:
        dl = 0

    for word in q_words:
        if word in term_freq_dict:
            df_t = len(term_doc_freq_dict[word])
            if document_id in term_doc_freq_dict[word]:
                tf_t_d = term_doc_freq_dict[word][document_id]
            else:
                tf_t_d = 0
        else:
            df_t = 0
            tf_t_d = 0
        result += math.log(total_num_of_documents/(df_t+epsilon)) * (k + 1) * tf_t_d /(k*(1-b + b*dl/dl_avg) + tf_t_d + epsilon)
    return result

        


spark = SparkSession.builder \
    .appName('query_searching') \
    .config("spark.cassandra.connection.host", "cassandra-server") \
    .getOrCreate()

sc = spark.sparkContext

query = " ".join(sys.argv[1:])

tokenized_query = tokenize_text(query)
print("Tokenized query:")
print(tokenized_query)
quoted_words = []
for word in tokenized_query:
    quoted_words.append(f"'{word}'")

term_list_string = ", ".join(quoted_words)
term_condition = f"term IN ({term_list_string})"
# get term_freq
term_freq = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="term_freq", keyspace="search_engine") \
        .load() \
        .filter(term_condition) \
        .rdd \
        .map(lambda row: (row.term, row.term_freq)) \
        .collectAsMap()

# get term_doc_freq
term_doc_freq = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="term_doc_freq", keyspace="search_engine") \
        .load() \
        .filter(term_condition) \
        .rdd \
        .map(lambda row: (row.term, {row.document_id: row.term_in_doc_frequency})) \
        .reduceByKey(lambda a, b: {**a, **b}) \
        .collectAsMap()

# get doc_length
# doc_length = spark.read \
#         .format("org.apache.spark.sql.cassandra") \
#         .options(table="doc_length", keyspace="search_engine") \
#         .load() \
#         .rdd \
#         .map(lambda row: (row.document_id, row.document_length)) \
#         .collectAsMap()

doc_info = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="doc_length", keyspace="search_engine") \
        .load() \
        .rdd \
        .collect()


doc_length_dict = {row.document_id: row.document_length for row in doc_info}
doc_titles_dict = {row.document_id: row.document_title for row in doc_info if hasattr(row, 'document_title')}


general_stats = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="general_stats", keyspace="search_engine") \
        .load() \
        .filter("statistics_name IN ('num_of_documents', 'avg_doc_length')") \
        .rdd \
        .map(lambda row: (row.statistics_name, row.statistics_value)) \
        .collectAsMap()

N = int(general_stats.get('num_of_documents', 1))
avg_dl = float(general_stats.get('avg_doc_length', 1.0))

print("N:", N)
print("avg_dl:", avg_dl)

relevant_docs = set()
for documents in term_doc_freq.values():
    relevant_docs.update(documents.keys())

print("Relevant documents:")
print(relevant_docs)
# process relevant documents using Spark
relevant_document_ids_rdd = sc.parallelize(list(relevant_docs))
relevant_document_metrics = relevant_document_ids_rdd.map(lambda document_id: (document_id, compute_BM25(N, avg_dl, tokenized_query, term_freq, term_doc_freq, doc_length_dict, document_id)))
relevant_documents = relevant_document_metrics.sortBy(lambda x: x[1], ascending=False).take(10)

print("Found 10 relevant documents:")

for document_id, score in relevant_documents:
    print(f"Document id: {document_id}, title: {doc_titles_dict.get(document_id, 'not stated')},  score: {score}")

# store the result in hdfs
result_path = "/tmp/index/top_10"

results_rdd = sc.parallelize(relevant_documents)
results_rdd.saveAsTextFile(result_path)

spark.stop()




