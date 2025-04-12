#!/usr/bin/env python3
import re
import sys

def tokenize_text(text):
    """Tokenize text into words, returning only alphanumeric terms"""
    return re.findall(r'\w+', text.lower())

# Process input from tab-separated files in /index/data
for line in sys.stdin:
    line = line.strip()
    
    # Skip empty lines
    if not line:
        continue
        
    # Each line format is: <doc_id>\t<doc_title>\t<doc_text>
    try:
        parts = line.split('\t', 2)
        
        if len(parts) < 3:
            print(f"Warning: Malformed input line, expected 3 tab-separated fields, got {len(parts)}", file=sys.stderr)
            continue
            
        document_id, doc_title, doc_text = parts
        
        tokenized_title = tokenize_text(doc_title)
        tokenized_text = tokenize_text(doc_text)
        
        # # Emit title terms (could be weighted higher)
        # for word in tokenized_title:
        #     if len(word) >= 2:  # Skip single characters
        #         print(f'{word}\t{document_id}')
        
        for word in tokenized_text:
            print(f'{word}\t{document_id}')
                
    except Exception as e:
        print(f"Error processing line: {e}", file=sys.stderr)