#!/usr/bin/env python3
import sys
import json

def is_expr(node):
    return isinstance(node, dict) and node.get("type") == "expr"

def collect_strings_via_items_chain(node, out):
    """
    Suivre UNIQUEMENT la chaîne 'items' jusqu'à atteindre une liste de chaînes.
    """
    if is_expr(node):
        return

    if isinstance(node, dict):
        if "items" in node:
            collect_strings_via_items_chain(node["items"], out)

    elif isinstance(node, list):
        if not node:
            return
        any_dict = any(isinstance(it, dict) for it in node)
        if any_dict:
            for it in node:
                if isinstance(it, dict) and "items" in it:
                    collect_strings_via_items_chain(it["items"], out)
        else:
            if all(isinstance(it, str) for it in node):
                out.extend(node)

def take_text_from_text_widget(block, results):
    bdesign = block.get("design", {})
    items = bdesign.get("items")
    if items is not None:
        collect_strings_via_items_chain(items, results)

def take_text_from_pivotchart(block, results, legends):
    bdesign = block.get("design", {})
    header_items = bdesign.get("header", {}).get("items")
    footer_items = bdesign.get("footer", {}).get("items")
    if header_items is not None:
        collect_strings_via_items_chain(header_items, results)
    if footer_items is not None:
        collect_strings_via_items_chain(footer_items, results)
    y_label = bdesign.get("yAxisLabelText")
    if isinstance(y_label, str) and y_label.strip():
        legends.append(y_label)

def take_text_from_layeredchart(block, results, legends):
    bdesign = block.get("design", {})
    header_items = bdesign.get("header", {}).get("items")
    footer_items = bdesign.get("footer", {}).get("items")
    if header_items is not None:
        collect_strings_via_items_chain(header_items, results)
    if footer_items is not None:
        collect_strings_via_items_chain(footer_items, results)
    y_label = bdesign.get("yAxisLabelText")
    if isinstance(y_label, str) and y_label.strip():
        legends.append(y_label)
    for layer in bdesign.get("layers", []) or []:
        name = layer.get("name")
        if isinstance(name, str) and name.strip():
            legends.append(name)

def take_text_from_tablechart(block, results, legends):
    bdesign = block.get("design", {})
    title = bdesign.get("titleText")
    if isinstance(title, str):
        results.append(title)
    elif isinstance(title, dict) and "items" in title:
        collect_strings_via_items_chain(title["items"], results)
    y_label = bdesign.get("yAxisLabelText")
    if isinstance(y_label, str) and y_label.strip():
        legends.append(y_label)

def take_legends_from_map(block, legends):
    # Map: design.layerViews[].name
    bdesign = block.get("design", {})
    for lv in bdesign.get("layerViews", []) or []:
        name = lv.get("name")
        if isinstance(name, str) and name.strip():
            legends.append(name)

def collect_visual_type(block, visual_types):
    """
    Ajoute un libellé de type visuel selon widgetType.
    - Map -> "map"
    - TableChart -> "table"
    - PivotChart -> "pivot table"
    - LayeredChart -> design.type (ex: "bar")
    """
    wtype = block.get("widgetType")
    if wtype == "Map":
        visual_types.append("map")
    elif wtype == "TableChart":
        visual_types.append("table")
    elif wtype == "PivotChart":
        visual_types.append("pivot table")
    elif wtype == "LayeredChart":
        bdesign = block.get("design", {})
        vtype = bdesign.get("type")
        if isinstance(vtype, str) and vtype.strip():
            visual_types.append(vtype.strip())

def process_blocks(blocks, results, legends, visual_types):
    """Parcourt récursivement la hiérarchie de blocks."""
    if not isinstance(blocks, list):
        return
    for block in blocks:
        if not isinstance(block, dict):
            continue

        # Collecter le type visuel de ce block s'il est pertinent
        collect_visual_type(block, visual_types)

        # Descente récursive si sous-conteneurs
        if "blocks" in block:
            process_blocks(block.get("blocks", []), results, legends, visual_types)

        # Widgets: extraire textes/légendes
        wtype = block.get("widgetType")
        if wtype == "Text":
            take_text_from_text_widget(block, results)
        elif wtype == "PivotChart":
            take_text_from_pivotchart(block, results, legends)
        elif wtype == "LayeredChart":
            take_text_from_layeredchart(block, results, legends)
        elif wtype == "TableChart":
            take_text_from_tablechart(block, results, legends)
        elif wtype == "Map":
            take_legends_from_map(block, legends)
        # autres widgetTypes ignorés

def process_tab(tab, results, legends, visual_types):
    if tab.get("type") != "dashboard":
        return

    # design.tabs.name
    name = tab.get("name")
    if isinstance(name, str):
        results.append(name)

    # design.tabs.design.items.blocks (avec blocs imbriqués)
    blocks = (
        tab.get("design", {})
           .get("items", {})
           .get("blocks", [])
    )
    process_blocks(blocks, results, legends, visual_types)

def unique_preserve_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def process_document(doc):
    doc_id = doc.get("_id", "unknown")
    tabs = doc.get("design", {}).get("tabs", [])

    texts = []
    legends = []
    visual_types = []
    for tab in tabs:
        process_tab(tab, texts, legends, visual_types)

    # Émission des textes
    for idx, t in enumerate(texts):
        print(f"{doc_id}\t{idx}\t{t}")

    # Légendes
    legends = [s.strip() for s in legends if isinstance(s, str) and s.strip()]
    legends = unique_preserve_order(legends)
    next_index = len(texts)
    if legends:
        print(f"{doc_id}\t{next_index}\tLegendes : {' ; '.join(legends)}")
        next_index += 1

    # Types de visuels
    visual_types = [s.strip() for s in visual_types if isinstance(s, str) and s.strip()]
    visual_types = unique_preserve_order(visual_types)
    if visual_types:
        print(f"{doc_id}\t{next_index}\tType de visuels : {' ; '.join(visual_types)}")

def main():
    raw = sys.stdin.read().strip()
    if not raw:
        return
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            for d in parsed:
                process_document(d)
        elif isinstance(parsed, dict):
            process_document(parsed)
        else:
            raise ValueError("Format JSON inattendu.")
    except json.JSONDecodeError:
        # JSONL fallback
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
                process_document(d)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
