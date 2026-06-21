"""Generate + enrich descriptions for the undescribed GA360 GA_SESSIONS nested
VARIANT fields.

Approach (per request):
  1. canonical meaning from the GA export schema doc (google_analytics_sample.ga_sessions.md)
  2. live profiling of the actual Snowflake data (sample values, ranges, null rate)
  3. benchmark usage hints
These are combined into one <=500-char description per field, written to the
ChromaDB schema_cards (comment + document) and mirrored to a JSON sidecar.

Usage:
  uv run python -m scripts.describe_ga360_nested            # generate + show
  uv run python -m scripts.describe_ga360_nested --apply    # also enrich Chroma
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

MAXLEN = 500
PROFILE_PATH = Path("/tmp/ga360_profile.json")
SIDECAR = Path(__file__).resolve().parent.parent / "data" / "ga360_nested_descriptions.json"

# 1) Canonical semantic meaning (condensed from the GA BigQuery export schema doc).
SEMANTIC = {
    "totals.hits": "Total number of hits within the session (aggregate over the session).",
    "totals.newVisits": "Convenience flag for new users: 1 if this is the user's first visit, otherwise NULL.",
    "totals.pageviews": "Total number of pageviews within the session.",
    "totals.timeOnSite": "Total session duration in seconds.",
    "totals.visits": "Session-count convenience flag: 1 for sessions with interaction events, NULL otherwise.",
    "trafficSource.campaign": "Marketing campaign name (utm_campaign); '(not set)' when none.",
    "trafficSource.keyword": "Keyword of the traffic source (utm_term), usually set when medium is 'organic' or 'cpc'; often '(not provided)'.",
    "trafficSource.medium": "Medium of the traffic source: 'organic', 'cpc', 'referral', 'affiliate', 'cpm', or '(none)' for direct.",
    "trafficSource.source": "Source of the traffic: search-engine name, referring hostname, or utm_source value. Pairs with trafficSource:medium.",
    "trafficSource.adwordsClickInfo": "Nested RECORD of Google Ads click details (adGroupId, campaignId, creativeId, gclId, page, slot, isVideoAd, targetingCriteria) under last-non-direct attribution.",
    "device.browser": "Web browser used (e.g., Chrome, Safari, Firefox).",
    "device.browserSize": "Viewport size of the browser as width x height in pixels.",
    "device.browserVersion": "Version of the browser used.",
    "device.deviceCategory": "Device form factor: 'desktop', 'mobile', or 'tablet'. Preferred over the deprecated device:isMobile.",
    "device.flashVersion": "Version of the Adobe Flash plugin installed in the browser.",
    "device.isMobile": "Deprecated BOOLEAN: true if the user is on a mobile device, else false. Use device:deviceCategory instead.",
    "device.language": "Device language as an IETF language code.",
    "device.mobileDeviceBranding": "Brand or manufacturer of the mobile device.",
    "device.mobileDeviceInfo": "Branding, model, and marketing name identifying the mobile device.",
    "device.mobileDeviceMarketingName": "Marketing name of the mobile device.",
    "device.mobileDeviceModel": "Mobile device model.",
    "device.mobileInputSelector": "Mobile input selector used (e.g., touchscreen, joystick, clickwheel, stylus).",
    "device.operatingSystem": "Operating system of the device (e.g., Windows, Macintosh, Android, iOS).",
    "device.operatingSystemVersion": "Version of the operating system.",
    "device.screenColors": "Display color depth (bit-depth, e.g., '24-bit').",
    "device.screenResolution": "Screen resolution as pixel width x height (e.g., '800x600').",
    "geoNetwork.city": "City the session originated from, derived from IP address / Geo ID.",
    "geoNetwork.cityId": "City ID derived from IP address / Geo ID.",
    "geoNetwork.continent": "Continent the session originated from, based on IP address.",
    "geoNetwork.country": "Country the session originated from, based on IP address.",
    "geoNetwork.latitude": "Approximate latitude of the user's city (STRING); positive north, negative south of the equator.",
    "geoNetwork.longitude": "Approximate longitude of the user's city (STRING); positive east, negative west of the prime meridian.",
    "geoNetwork.metro": "Designated Market Area (DMA) the session originated from.",
    "geoNetwork.networkDomain": "Domain name of the user's ISP (legacy; no longer supported).",
    "geoNetwork.networkLocation": "Service-provider name used to reach the property (legacy; no longer supported).",
    "geoNetwork.region": "Region the session originated from (e.g., a U.S. state).",
    "geoNetwork.subContinent": "Sub-continent the session originated from, based on IP address.",
}

PARENT_TYPE = {  # fallback access-hint type when no profile is available
    "totals": "INTEGER aggregate", "trafficSource": "STRING", "device": "STRING", "geoNetwork": "STRING",
}
KIND_TYPE = {"num": "INTEGER aggregate", "str": "STRING", "bool": "BOOLEAN", "obj": "RECORD"}


def _observed_clause(field: str, prof: dict) -> str:
    e = prof.get(field)
    if not e:
        return ""
    nr = e.get("null_rate")
    kind = e.get("kind")
    if kind == "num":
        return f"Observed range {e.get('min')}-{e.get('max')} (avg {e.get('avg')}); null_rate {nr}."
    if kind == "bool":
        vals = e.get("values", {})
        return f"Observed {vals}; null_rate {nr}."
    if kind == "obj":
        return "Values redacted in this demo dataset ('not available in demo dataset')."
    # string
    top = e.get("top", [])
    if e.get("distinct") == 1 and top and "not available in demo dataset" in str(top[0][0]):
        return "Not populated in this demo dataset (constant 'not available in demo dataset')."
    vals = ", ".join(str(v) for v, _ in top[:6])
    return f"Observed values: {vals} ({e.get('distinct')} distinct); null_rate {nr}."


def build_descriptions(profile: dict) -> dict[str, str]:
    out = {}
    for field, meaning in SEMANTIC.items():
        parent = field.split(".")[0]
        sub = field.split(".")[1]
        ftype = KIND_TYPE.get((profile.get(field) or {}).get("kind"), PARENT_TYPE[parent])
        access = f'Access "{parent}":{sub} ({ftype}).'
        observed = _observed_clause(field, profile)
        desc = f"{meaning} {access} {observed}".strip()
        if len(desc) > MAXLEN:
            desc = desc[: MAXLEN - 1].rstrip() + "…"
        out[field] = desc
    return out


def enrich_chroma(descs: dict[str, str], update_documents: bool = False) -> dict:
    """Write descriptions into the GA360 VARIANT_FIELD cards.

    By default updates METADATA ONLY (the ``comment`` the LLM reads in the schema
    slice). Updating ``documents`` re-embeds via the OpenAI embedding API; pass
    ``update_documents=True`` only when embedding quota is available, to also
    improve semantic-search ranking.
    """
    from rag_snow_agent.chroma.chroma_store import ChromaStore
    store = ChromaStore(persist_dir=".chroma")
    col = store.schema_collection()
    total = col.count(); off = 0; BATCH = 20000
    updates = {"ids": [], "metadatas": [], "documents": []}
    while off < total:
        res = col.get(limit=BATCH, offset=off, include=["metadatas", "documents"])
        ids = res.get("ids") or []
        ms = res.get("metadatas") or []
        ds = res.get("documents") or []
        for cid, m, d in zip(ids, ms, ds):
            if m.get("db_id") != "GA360" or m.get("data_type") != "VARIANT_FIELD":
                continue
            qn = m.get("qualified_name", "")
            tail = qn.split("GA_SESSIONS_", 1)[-1]
            # tail like '20170630."totals":pageviews'
            path = tail.split(".", 1)[-1].replace('"', "")  # 'totals:pageviews'
            if ":" not in path:
                continue
            parent, sub = path.split(":", 1)
            key = f"{parent}.{sub}"
            desc = descs.get(key)
            if not desc:
                continue
            new_m = dict(m)
            new_m["comment"] = desc
            new_doc = (
                f"Column: {qn}\nType: VARIANT_FIELD\nDescription: {desc}\nNullable: YES"
            )
            updates["ids"].append(cid)
            updates["metadatas"].append(new_m)
            updates["documents"].append(new_doc)
        off += len(ids)
    if updates["ids"]:
        if update_documents:
            col.update(ids=updates["ids"], metadatas=updates["metadatas"],
                       documents=updates["documents"])
        else:
            # metadata-only: no re-embedding (avoids embedding-API calls/quota)
            col.update(ids=updates["ids"], metadatas=updates["metadatas"])
    return {"updated": len(updates["ids"]), "documents_reembedded": update_documents}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Write descriptions into ChromaDB (metadata only)")
    ap.add_argument("--reembed", action="store_true", help="Also re-embed document text (needs OpenAI embedding quota)")
    args = ap.parse_args()

    profile = json.loads(PROFILE_PATH.read_text()) if PROFILE_PATH.exists() else {}
    descs = build_descriptions(profile)

    longest = max(len(v) for v in descs.values())
    print(f"Generated {len(descs)} descriptions; longest = {longest} chars (limit {MAXLEN})")
    for k, v in descs.items():
        print(f"\n[{k}] ({len(v)} chars)\n  {v}")

    SIDECAR.write_text(json.dumps(descs, indent=2) + "\n")
    print(f"\nWrote sidecar: {SIDECAR}")

    if args.apply:
        res = enrich_chroma(descs, update_documents=args.reembed)
        print(f"ChromaDB updated: {res}")


if __name__ == "__main__":
    main()
