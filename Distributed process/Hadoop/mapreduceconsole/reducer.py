#!/usr/bin/env python3
import sys
from collections import defaultdict

# Map: {doc_id: [(index, text), ...]}
documents = defaultdict(list)

for line in sys.stdin:
    parts = line.rstrip("\n").split("\t", 2)
    if len(parts) != 3:
        continue
    doc_id, idx_str, text = parts
    try:
        idx = int(idx_str)
    except ValueError:
        continue
    documents[doc_id].append((idx, text))

# Sort by doc_id (stable), then by index
for doc_id in sorted(documents.keys()):
    for _, text in sorted(documents[doc_id], key=lambda x: x[0]):
        print(f"{doc_id}\t{text}")
