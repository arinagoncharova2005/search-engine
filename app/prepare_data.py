from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


df = spark.read.parquet("/a.parquet")
n = 1000
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)


# get document_id, docunent_title and document_content
def process_document(path_to_file, document_content):
    filename = os.path.basename(path_to_file)
    document_id, document_title = filename.split('_', 1)   
    
    return (document_id, document_title, document_content)

sc = spark.sparkContext
print("Process documents to put them into hdfs")

# get text files from /data directory in HDFS
input_path = "/data"
print(f"Reading documents from {input_path}")

#  get (filename, content) pairs
documents_rdd = sc.wholeTextFiles(input_path)


# from document filename and content create (document_id, document_title, document_content)
processed_rdd = documents_rdd.map(lambda x: process_document(x[0], x[1]))

# make (document_id, document_title, document_content) separated with tabs
formatted_rdd = processed_rdd.map(lambda x: f"{x[0]}\t{x[1]}\t{x[2]}")

output_path = "/index/data"
print(f"Putting documents into {output_path}")

# merge files to one partition
formatted_rdd.coalesce(1).saveAsTextFile(output_path)

print("Documents are prepared!")
spark.stop()

