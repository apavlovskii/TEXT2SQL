"""Card + describe the GA360 GA_SESSIONS `hits` ARRAY nested fields.

`hits` is a repeated RECORD (array of structs) — its sub-fields were never
carded, so analytics queries needing product revenue / ecommerce actions / page
paths had no schema support. This profiles the real nested structure, generates
<=500-char descriptions (canonical meaning + observed values + LATERAL FLATTEN
access path), and CREATES VARIANT_FIELD cards in ChromaDB.

  uv run python -m scripts.describe_ga360_hits           # profile + show
  uv run python -m scripts.describe_ga360_hits --apply   # also create cards (embeds)
"""
from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

TABLE = "GA360.GOOGLE_ANALYTICS_SAMPLE.GA_SESSIONS_20170630"
MAXLEN = 500
SIDECAR = Path(__file__).resolve().parent.parent / "data" / "ga360_hits_descriptions.json"

# group: hit (h.value:leaf) | obj (h.value:parent.leaf) | product (FLATTEN product → p.value:leaf)
# (group, parent, leaf, kind, meaning)
FIELDS = [
    ("hit", "", "type", "str", "Type of hit: e.g. PAGE, EVENT, TRANSACTION, ITEM, SOCIAL, APPVIEW."),
    ("hit", "", "hitNumber", "num", "Sequenced hit number within the session; 1 for the first hit."),
    ("hit", "", "hour", "num", "Hour the hit occurred (0-23)."),
    ("hit", "", "minute", "num", "Minute the hit occurred (0-59)."),
    ("hit", "", "time", "num", "Milliseconds after the session's first hit when this hit occurred; 0 for the first hit."),
    ("hit", "", "isEntrance", "bool", "True if this hit was the first pageview/screenview of the session."),
    ("hit", "", "isExit", "bool", "True if this hit was the last pageview/screenview of the session."),
    ("hit", "", "isInteraction", "bool", "True if the hit was an interaction hit."),
    ("hit", "", "dataSource", "str", "Source of the hit: 'web' (analytics.js) or 'app' (mobile SDK)."),
    ("obj", "page", "pagePath", "str", "URL path of the page for this hit (e.g., /home)."),
    ("obj", "page", "pageTitle", "str", "Title of the page for this hit."),
    ("obj", "page", "hostname", "str", "Hostname of the page for this hit."),
    ("obj", "page", "pagePathLevel1", "str", "First level of the page-path hierarchy."),
    ("obj", "page", "searchKeyword", "str", "On-site search keyword for the page, if any."),
    ("obj", "eCommerceAction", "action_type", "str", "Ecommerce action: 1=list click, 2=detail view, 3=add-to-cart, 4=remove-from-cart, 5=checkout, 6=purchase, 7=refund, 8=checkout-option, 0=unknown."),
    ("obj", "eCommerceAction", "step", "num", "Checkout step number when a checkout step is specified."),
    ("obj", "eCommerceAction", "option", "str", "Checkout option label (e.g., a shipping option)."),
    ("obj", "transaction", "transactionId", "str", "Unique transaction/order ID."),
    ("obj", "transaction", "transactionRevenue", "num", "Total transaction revenue in micros (actual value x 10^6)."),
    ("obj", "transaction", "transactionShipping", "num", "Shipping amount in micros (x 10^6)."),
    ("obj", "transaction", "transactionTax", "num", "Tax amount in micros (x 10^6)."),
    ("obj", "transaction", "currencyCode", "str", "ISO currency code of the transaction."),
    ("obj", "transaction", "affiliation", "str", "Store or affiliation for the transaction."),
    ("obj", "eventInfo", "eventCategory", "str", "Event category."),
    ("obj", "eventInfo", "eventAction", "str", "Event action."),
    ("obj", "eventInfo", "eventLabel", "str", "Event label."),
    ("product", "product", "v2ProductName", "str", "Product name."),
    ("product", "product", "v2ProductCategory", "str", "Product category."),
    ("product", "product", "productSKU", "str", "Product SKU / code."),
    ("product", "product", "productRevenue", "num", "Revenue attributed to the product, in micros (value x 10^6)."),
    ("product", "product", "productQuantity", "num", "Quantity of the product in the action."),
    ("product", "product", "productPrice", "num", "Product unit price in micros (value x 10^6)."),
    ("product", "product", "productBrand", "str", "Product brand."),
    ("product", "product", "isImpression", "bool", "TRUE if the product was seen (impression) in a product list."),
    ("product", "product", "isClick", "bool", "TRUE if the product was clicked in a product list."),
    ("product", "product", "productListName", "str", "Name of the product list where the product appeared."),
    ("product", "product", "productListPosition", "num", "Position of the product within its list."),
    ("product", "product", "productVariant", "str", "Product variant."),
]


def keypath(g, parent, leaf):
    """Card key under the hits parent (no leading table)."""
    return leaf if g == "hit" else f"{parent}.{leaf}"


def access_hint(g, parent, leaf):
    if g == "hit":
        return f'Repeated array — LATERAL FLATTEN(input=>"hits") h, then h.value:{leaf}.'
    if g == "obj":
        return f'Repeated array — after LATERAL FLATTEN(input=>"hits") h, access h.value:{parent}.{leaf}.'
    return (f'Repeated nested array — LATERAL FLATTEN(input=>"hits") h, then '
            f'LATERAL FLATTEN(input=>h.value:product) p, access p.value:{leaf}.')


def cast(kind):
    return {"num": "::int", "str": "::string", "bool": "::boolean"}[kind]


def profile(ex):
    """Return {f'{g}:{parent}:{leaf}': profile_entry} from live data."""
    out = {}

    def run(rows_sql, items):
        r = ex.execute(rows_sql)
        if not r.success:
            print("PROFILE QUERY FAILED:", (r.error_message or "")[:200])
            return
        cols = [c.lower() for c in (r.column_names or [])]
        idx = {c: i for i, c in enumerate(cols)}
        rows = r.rows_sample or []
        n = len(rows)
        for (g, parent, leaf, kind, _), alias in items:
            i = idx.get(alias.lower())
            vals = [row[i] for row in rows] if i is not None else []
            nn = [v for v in vals if v is not None]
            e = {"kind": kind, "null_rate": round(1 - len(nn) / max(n, 1), 3), "n": n}
            if kind == "num" and nn:
                nums = [float(v) for v in nn]
                e["min"], e["max"], e["avg"] = int(min(nums)), int(max(nums)), round(sum(nums) / len(nums), 1)
            elif kind == "bool":
                e["values"] = dict(collections.Counter(str(v) for v in nn))
            else:
                c = collections.Counter(str(v) for v in nn)
                e["distinct"] = len(c); e["top"] = c.most_common(6)
            out[f"{g}:{parent}:{leaf}"] = e

    hit_items = [(f, f"a_{f[2]}") for f in FIELDS if f[0] in ("hit", "obj")]
    sel = ", ".join(
        (f'h.value:{f[2]}{cast(f[3])}' if f[0] == "hit"
         else f'h.value:{f[1]}.{f[2]}{cast(f[3])}') + f' AS "a_{f[2]}"'
        for f, _ in hit_items
    )
    run(f'SELECT {sel} FROM {TABLE}, LATERAL FLATTEN(input=>"hits") h LIMIT 12000', hit_items)

    prod_items = [(f, f"a_{f[2]}") for f in FIELDS if f[0] == "product"]
    selp = ", ".join(f'p.value:{f[2]}{cast(f[3])} AS "a_{f[2]}"' for f, _ in prod_items)
    run(f'SELECT {selp} FROM {TABLE}, LATERAL FLATTEN(input=>"hits") h, '
        f'LATERAL FLATTEN(input=>h.value:product) p LIMIT 12000', prod_items)
    return out


def observed_clause(e):
    if not e:
        return ""
    nr = e.get("null_rate")
    if e["kind"] == "num" and "min" in e:
        return f"Observed {e['min']}-{e['max']} (avg {e['avg']}); null_rate {nr}."
    if e["kind"] == "bool":
        return f"Observed {e.get('values')}; null_rate {nr}."
    top = e.get("top", [])
    if not top:
        return f"null_rate {nr}."
    vals = ", ".join(str(v) for v, _ in top[:5])
    return f"Observed: {vals} ({e.get('distinct')} distinct); null_rate {nr}."


def build(profile_data):
    out = {}
    for g, parent, leaf, kind, meaning in FIELDS:
        key = keypath(g, parent, leaf)
        e = profile_data.get(f"{g}:{parent}:{leaf}")
        desc = f"{meaning} {access_hint(g, parent, leaf)} {observed_clause(e)}".strip()
        if len(desc) > MAXLEN:
            desc = desc[: MAXLEN - 1].rstrip() + "…"
        out[key] = desc
    return out


def create_cards(descs):
    from rag_snow_agent.chroma.chroma_store import ChromaStore
    store = ChromaStore(persist_dir=".chroma")
    col = store.schema_collection()
    ids, docs, metas = [], [], []
    for key, desc in descs.items():
        qn = f'{TABLE}."hits":{key}'
        ids.append(f"column:{qn}")
        metas.append({
            "db_id": "GA360",
            "qualified_name": qn,
            "table_qualified_name": TABLE,
            "object_type": "column",
            "data_type": "VARIANT_FIELD",
            "source": "profiled",
            "comment": desc,
            "token_estimate": max(1, len(desc) // 4),
        })
        docs.append(f"Column: {qn}\nType: VARIANT_FIELD\nDescription: {desc}\nNullable: YES")
    col.upsert(ids=ids, documents=docs, metadatas=metas)  # embeds documents
    return {"created": len(ids)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    from rag_snow_agent.snowflake.executor import SnowflakeExecutor
    ex = SnowflakeExecutor(credentials_path="snowflake_credentials.json", db_id="GA360", sample_rows=12000)
    prof = profile(ex)
    ex.close()

    descs = build(prof)
    longest = max(len(v) for v in descs.values())
    print(f"Generated {len(descs)} hits.* descriptions; longest {longest} chars (limit {MAXLEN})")
    for k, v in descs.items():
        print(f"\n[hits:{k}] ({len(v)})\n  {v}")
    SIDECAR.write_text(json.dumps(descs, indent=2) + "\n")
    print(f"\nWrote sidecar: {SIDECAR}")
    if args.apply:
        print("ChromaDB:", create_cards(descs))


if __name__ == "__main__":
    main()
