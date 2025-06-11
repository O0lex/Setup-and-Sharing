import pandas as pd
import requests
from time import sleep
from tqdm import tqdm

# === Authentication Setup ===
CLIENT_ID = "your_client_id_here"
CLIENT_SECRET = "your_client_secret_here"

def get_access_token():
    r = requests.get("https://api.locallogic.co/oauth/token", params={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    })
    r.raise_for_status()
    return r.json()["access_token"]

def make_headers():
    token = get_access_token()
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

BASE = "https://api.locallogic.co/v3"

# === Load Input File ===
df = pd.read_csv("/mnt/data/minimapping.csv")

# === Prepare Columns ===
df["new_geo_id"] = None
df["location_scores"] = None
df["demographics"] = None
df["value_drivers"] = None
df["pois"] = None
df["poi_success"] = 0

# === API Call Functions ===
def fetch_geography(lat, lng, headers):
    params = {"lat": lat, "lng": lng, "levels": "10"}
    r = requests.get(f"{BASE}/geographies", headers=headers, params=params)
    r.raise_for_status()
    geogs = r.json()["data"]["geographies"]
    if not geogs:
        return None
    return next(iter(geogs))

def fetch_scores(geo_id, headers):
    r = requests.get(f"{BASE}/scores", headers=headers, params={"geography_ids": geo_id})
    r.raise_for_status()
    return r.json()["data"]

def fetch_demographics(geo_id, headers):
    r = requests.get(f"{BASE}/demographics/{geo_id}", headers=headers)
    r.raise_for_status()
    return r.json()["data"]["attributes"]

def fetch_value_drivers(geo_id, headers):
    r = requests.get(f"{BASE}/value-drivers/{geo_id}", headers=headers)
    r.raise_for_status()
    return r.json()["data"]["value_drivers"]

def fetch_pois(lat, lng, headers):
    try:
        r = requests.get(f"{BASE}/enhanced-pois", headers=headers, params={"lat": lat, "lng": lng, "radius": 1000})
        r.raise_for_status()
        pois = r.json()["data"]["results"]
        counts = r.json()["meta"]["counts"]["total_by_category"]
        return pois, counts, 1
    except:
        return [], {}, 0

# === Enrichment Loop ===
for idx, row in tqdm(df.iterrows(), total=len(df), desc="Enriching neighborhoods"):
    lat, lng = row["latitude"], row["longitude"]
    try:
        headers = make_headers()
        geo_id = fetch_geography(lat, lng, headers)
        df.at[idx, "new_geo_id"] = geo_id

        if geo_id:
            df.at[idx, "location_scores"]  = fetch_scores(geo_id, headers)
            df.at[idx, "demographics"]     = fetch_demographics(geo_id, headers)
            df.at[idx, "value_drivers"]    = fetch_value_drivers(geo_id, headers)

        pois, counts, ok = fetch_pois(lat, lng, headers)
        df.at[idx, "pois"] = pois
        df.at[idx, "poi_success"] = ok
        for cat, n in counts.items():
            df.at[idx, f"poi_{cat}_count"] = n

        sleep(0.3)
    except Exception as e:
        print(f"[{idx}] Error: {e}")

# === Flatten `location_scores` ===
score_rows = []
for item in df["location_scores"]:
    rec = {}
    if isinstance(item, dict):
        loc = item.get("location", {})
        for k,v in loc.items():
            rec[f"location_scores.{k}"] = v.get("value")
    score_rows.append(rec)
scores_df = pd.DataFrame(score_rows)

# === Flatten `demographics` ===
demo_rows = []
for item in df["demographics"]:
    rec = {}
    if isinstance(item, dict):
        for cat, cat_val in item.items():
            for var in cat_val.get("variables", []):
                rec[f"demographics.{cat}.{var['variable']}"] = var.get("value")
    demo_rows.append(rec)
demo_df = pd.DataFrame(demo_rows)

# === Flatten `value_drivers` ===
vd_df = pd.json_normalize(df["value_drivers"].dropna().tolist()).add_prefix("value_drivers.")
# fill blanks if some rows had no drivers
vd_df = vd_df.reindex(range(len(df)))

# === Compute POI count ===
df["pois.count"] = df["pois"].apply(lambda x: len(x) if isinstance(x, list) else 0)

# === Final Merge ===
df_flat = pd.concat([
    df.drop(columns=["location_scores","demographics","value_drivers","pois"]),
    scores_df, demo_df, vd_df
], axis=1)

df_flat.to_csv("neighborhoods_enriched.csv", index=False)
print("✅ neighborhoods_enriched.csv saved!")
