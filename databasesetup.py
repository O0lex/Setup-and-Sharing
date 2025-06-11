import pandas as pd
import requests
from time import sleep
from tqdm import tqdm

# === Auth Setup ===
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
df["poi_success"] = 0  # 1 = success, 0 = fail

# === API Call Functions ===
def fetch_geography(lat, lng, headers):
    params = {"lat": lat, "lng": lng, "levels": "10"}
    r = requests.get(f"{BASE}/geographies", headers=headers, params=params)
    r.raise_for_status()
    geogs = r.json()["data"]["geographies"]
    if not geogs:
        return None, None
    gid, meta = next(iter(geogs.items()))
    return gid, meta

def fetch_scores(geo_id, headers):
    params = {"geography_ids": geo_id}
    r = requests.get(f"{BASE}/scores", headers=headers, params=params)
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
    params = {"lat": lat, "lng": lng, "radius": 1000}
    try:
        r = requests.get(f"{BASE}/enhanced-pois", headers=headers, params=params)
        r.raise_for_status()
        pois = r.json()["data"]["results"]
        meta_counts = r.json()["meta"]["counts"]["total_by_category"]
        return pois, meta_counts, 1
    except Exception as e:
        print(f"POI fetch failed at ({lat},{lng}): {e}")
        return [], {}, 0

# === Enrichment Loop with Progress Bar ===
for idx, row in tqdm(df.iterrows(), total=len(df), desc="Enriching neighborhoods"):
    lat, lng = row["latitude"], row["longitude"]
    try:
        headers = make_headers()
        geo_id, _ = fetch_geography(lat, lng, headers)
        df.at[idx, "new_geo_id"] = geo_id

        if geo_id:
            df.at[idx, "location_scores"] = fetch_scores(geo_id, headers)
            df.at[idx, "demographics"] = fetch_demographics(geo_id, headers)
            df.at[idx, "value_drivers"] = fetch_value_drivers(geo_id, headers)

        pois, category_counts, poi_success = fetch_pois(lat, lng, headers)
        df.at[idx, "pois"] = pois
        df.at[idx, "poi_success"] = poi_success

        for cat, count in category_counts.items():
            df.at[idx, f"poi_{cat}_count"] = count

        sleep(0.3)

    except Exception as e:
        print(f"[{idx}] Failed: {e}")
        continue

# === Flattening Step ===
scores_df = pd.json_normalize(df["location_scores"]).add_prefix("location_scores.")
demo_df = pd.json_normalize(df["demographics"]).add_prefix("demographics.")

# Handle value_drivers row-wise
vd_df = pd.DataFrame()
for idx, item in df["value_drivers"].items():
    if isinstance(item, list):
        flattened = {f"value_drivers.{d['name']}": d["value"] for d in item if "name" in d and "value" in d}
        vd_df = pd.concat([vd_df, pd.DataFrame([flattened])], ignore_index=True)
    else:
        vd_df = pd.concat([vd_df, pd.DataFrame([{}])], ignore_index=True)

# Count POIs
df["pois.count"] = df["pois"].apply(lambda x: len(x) if isinstance(x, list) else 0)

# Final assembly
df_flat = pd.concat([
    df.drop(columns=["location_scores", "demographics", "value_drivers", "pois"]),
    scores_df, demo_df, vd_df
], axis=1)

# === Save Output ===
df_flat.to_csv("mini_mapping_enriched.csv", index=False)
print("✅ Success: Saved as mini_mapping_enriched.csv")
