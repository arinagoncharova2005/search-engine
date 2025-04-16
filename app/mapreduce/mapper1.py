#!/usr/bin/env python
import re
import sys

# extract words
def tokenize_text(text):
    return re.findall(r'\w+', text.lower())

# process data
for line in sys.stdin:
    line = line.strip()
        
    # input format is the following: <doc_id>\t<doc_title>\t<doc_text>
    try:
        parts = line.split('\t', 2)
        document_id, doc_title, doc_text = parts
        
        # tokenized_title = tokenize_text(doc_title)
        tokenized_text = tokenize_text(doc_text)
        
        for word in tokenized_text:
            print(f'{word}\t{document_id}\t{doc_title}')
                
    except Exception as e:
        print(f"Error in mapper1.py: {e}")