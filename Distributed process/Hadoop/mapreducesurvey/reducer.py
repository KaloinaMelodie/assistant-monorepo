#!/usr/bin/env python3
import sys
from collections import defaultdict

# Dictionnaire {doc_id: [(index, text), ...]}
documents = defaultdict(list)

for line in sys.stdin:
    parts = line.strip().split("\t", 3)
    if len(parts) != 3:
        continue

    doc_id, index_str, text = parts
    try:
        index = int(index_str)
    except ValueError:
        continue

    documents[doc_id].append((index, text))

# Tri et affichage par doc_id
for doc_id in sorted(documents.keys()):
    lines = sorted(documents[doc_id], key=lambda x: x[0])
    for _, text in lines:
        print(f"{doc_id}\t{text}")
