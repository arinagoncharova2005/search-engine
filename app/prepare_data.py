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


# df.write.csv("/index/data", sep = "\t")
## TO DO: change
def extract_document_id(filename):
    basename = os.path.basename(filename)
    parts = basename.split('_', 1)
    if len(parts) >= 1:
        return parts[0]
    else:
        return "no"


def process_document(file_path, file_content):
    """Extract document ID, title and content from the file"""
    doc_id = extract_document_id(file_path)
    
    # Simple title extraction - first non-empty line or filename
    lines = file_content.strip().split('\n')
    title = "Untitled"
    
    for line in lines:
        if line.strip():
            title = line.strip()
            break
    
    # Use filename as fallback title
    if title == "Untitled":
        basename = os.path.basename(file_path)
        if '_' in basename:
            title = basename.split('_', 1)[1]
            if title.endswith('.txt'):
                title = title[:-4]  # Remove .txt extension
    
    # Content is everything
    content = file_content.strip()
    
    return (doc_id, title, content)

sc = spark.sparkContext
print("Starting document preparation for HDFS...")

# Read all text files from /data directory in HDFS
input_path = "/data"
print(f"Reading documents from {input_path}")

# Use wholeTextFiles to get (filename, content) pairs
documents_rdd = sc.wholeTextFiles(input_path)

# Print count for debugging
doc_count = documents_rdd.count()
print(f"Found {doc_count} documents in HDFS")

# Transform: extract id, title, content
processed_rdd = documents_rdd.map(lambda x: process_document(x[0], x[1]))

# Convert to tab-separated format
formatted_rdd = processed_rdd.map(lambda x: f"{x[0]}\t{x[1]}\t{x[2]}")


# Ensure output directory exists
output_path = "/index/data"
print(f"Writing processed documents to {output_path}")

# Coalesce to one partition and save
formatted_rdd.coalesce(1).saveAsTextFile(output_path)

print("Document preparation completed successfully!")
spark.stop()

## TO DO: change
