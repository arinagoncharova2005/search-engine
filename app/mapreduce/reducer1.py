#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-server'])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
""")

session.execute('USE search_engine')

# drop tables if they exist
session.execute("DROP TABLE IF EXISTS term_freq;")
session.execute("DROP TABLE IF EXISTS term_doc_freq;")
session.execute("DROP TABLE IF EXISTS doc_length;")
session.execute("DROP TABLE IF EXISTS general_stats;")

# create term_freq table
session.execute("""
    CREATE TABLE IF NOT EXISTS term_freq (
        term TEXT PRIMARY KEY,
        term_freq INT
    );
""")
# create term_doc_freq table
session.execute("""
    CREATE TABLE IF NOT EXISTS term_doc_freq (
        term TEXT,
        document_id TEXT,
        term_in_doc_frequency INT,
        PRIMARY KEY (term, document_id)
    );
""")

# create doc_length freq
session.execute("""
    CREATE TABLE IF NOT EXISTS doc_length (
        document_id TEXT PRIMARY KEY,
        document_length INT
    );
""")

# create general_stats table
session.execute("""
    CREATE TABLE IF NOT EXISTS general_stats (
        statistics_name TEXT PRIMARY KEY,
        statistics_value DOUBLE
    );
""")

term_freq = {}
term_doc_freq = {}
doc_length = {}
already_processed_documents = set()

# processing data from mapper
for line in sys.stdin:
    print(line)
    word, document_id  = line.split('\t', 1)
    word = word.strip()
    if word in term_freq:
        term_freq[word] += 1
    else:
        term_freq[word] = 1
    if word not in term_doc_freq:
        term_doc_freq[word] = {}
    if document_id in term_doc_freq[word]:
        term_doc_freq[word][document_id] += 1
    else:
        term_doc_freq[word][document_id] = 1

    if document_id in doc_length:
        doc_length[document_id] += 1
    else:
        doc_length[document_id] = 1

    already_processed_documents.add(document_id)

print("Words are processed")

print('Insering into term_freq table')
for word, word_frequency in term_freq.items():
    session.execute("""
        INSERT INTO term_freq (term, term_freq)
        VALUES (%s, %s);
    """, (word, word_frequency))

print('Insering into term_doc_freq table')
for word, document_id_freq in term_doc_freq.items():
    for document_id, word_frequency in document_id_freq.items():
        session.execute("""
            INSERT INTO term_doc_freq (term, document_id, term_in_doc_frequency)
            VALUES (%s, %s, %s);
        """, (word, document_id, word_frequency))

print('Insering into doc_length table')
for document_id, document_length in doc_length.items():
    session.execute("""
        INSERT INTO doc_length (document_id, document_length)
        VALUES (%s, %s);
    """, (document_id, document_length))

num_of_documents = len(already_processed_documents)
avg_doc_length = sum(doc_length.values()) / num_of_documents

print('Insering num_of_documents into general_stats table')
session.execute("""
    INSERT INTO general_stats (statistics_name, statistics_value)
    VALUES ('num_of_documents', %s);
""", (num_of_documents,))

print('Insering avg_doc_length into general_stats table')
session.execute("""
    INSERT INTO general_stats (statistics_name, statistics_value)
    VALUES ('avg_doc_length', %s);
""", (avg_doc_length,))

# checking tables in Cassandra 
# select data from term_freq
rows = session.execute('SELECT * FROM term_freq LIMIT 5')
for row in rows:
    print(row)
   
# select data from term_doc_freq
rows = session.execute('SELECT * FROM term_doc_freq LIMIT 5')
for row in rows:
    print(row)

# select data from doc_length
rows = session.execute('SELECT * FROM doc_length LIMIT 5')
for row in rows:
    print(row)

# select data from general_stats
rows = session.execute('SELECT * FROM general_stats LIMIT 5')
for row in rows:
    print(row)

session.shutdown()
cluster.shutdown()


