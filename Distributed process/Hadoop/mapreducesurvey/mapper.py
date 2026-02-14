#!/usr/bin/env python3
import sys
import json

EXCLUDED_PATHS = [
    ["design", "description"],
    ["design", "calculations"],
    ["design", "localizedStrings"],
    ["dashboard"]
]

def is_excluded_path(current_path):
    """Vérifie si le chemin courant fait partie des chemins à exclure."""
    for excluded in EXCLUDED_PATHS:
        if current_path[:len(excluded)] == excluded:
            return True
    return False

def extract_lang_texts_ordered(obj, lang, ordered_texts, disabled_ancestor=False, path_stack=None):
    if path_stack is None:
        path_stack = []

    if isinstance(obj, dict):
        if obj.get("disabled", False) or disabled_ancestor or is_excluded_path(path_stack):
            return

        keys = list(obj.keys())
        priority_keys = []
        for key in ["name", "text"]:
            if key in keys:
                priority_keys.append(key)
                keys.remove(key)
        keys = priority_keys + keys 

        for key in keys:
            value = obj[key]
            new_path = path_stack + [key]
            if key == lang and isinstance(value, str):
                ordered_texts.append(value)
            else:
                extract_lang_texts_ordered(value, lang, ordered_texts, disabled_ancestor, new_path)

    elif isinstance(obj, list):
        for item in obj:
            extract_lang_texts_ordered(item, lang, ordered_texts, disabled_ancestor, path_stack)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        data = json.loads(line)
        doc_id = data.get("_id", "unknown")
        lang = data.get("design", {}).get("name", {}).get("_base", "fr")

        texts = []
        extract_lang_texts_ordered(data, lang, texts)

        for idx, text in enumerate(texts):
            print(f"{doc_id}\t{idx}\t{text}")

    except Exception as e:
        print(f"Error process ligne: {e}", file=sys.stderr)
