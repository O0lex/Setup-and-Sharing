import pandas as pd
import requests
from time import sleep

TOKEN = "Bearer YOUR_JWT_TOKEN_HERE"
HEADERS = {"Authorization": TOKEN, "Accept": "application/json"}
BASE = "https://api.locallogic.co/v3"

# Load your Excel file (converted to CSV first)
df = pd.read_csv("your_file.csv")  # Change this to the actual path

df["new_geo_id"] = None
df["location_scores"] = None
df["demographics"] = None
df["value_drivers"] = None
df["pois"] = None

def fetch_geography(lat, lng):
    params = {"lat": lat, "lng": lng, "levels": "10"}
    r = requests.get(f"{BASE}/geographies", headers=HEADERS, params=params)
    r.raise_for_status()
    geogs = r.json()["data"]["geographies"]
    if not geogs:
        return None, None
    gid, meta = next(iter(geogs.items()))
    return gid, meta

def fetch_scores(lat, lng, geo_id):
    params = {"lat": lat, "lng": lng, "geography_ids": geo_id}
    r = requests.get(f"{BASE}/scores", headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()["data"]

def fetch_demographics(geo_id):
    r = requests.get(f"{BASE}/demographics/{geo_id}", headers=HEADERS)
    r.raise_for_status()
    return r.json()["data"]["attributes"]

def fetch_value_drivers(geo_id):
    r = requests.get(f"{BASE}/value-drivers/{geo_id}", headers=HEADERS)
    r.raise_for_status()
    return r.json()["data"]["value_drivers"]

def fetch_pois(lat, lng):
    params = {"lat": lat, "lng": lng, "radius": 1000}
    r = requests.get(f"{BASE}/enhanced-pois", headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()["data"]["results"]

for idx, row in df.iterrows():
    lat, lng = row["latitude"], row["longitude"]
    try:
        geo_id, _ = fetch_geography(lat, lng)
        df.at[idx, "new_geo_id"] = geo_id

        if geo_id:
            df.at[idx, "location_scores"] = fetch_scores(lat, lng, geo_id)
            df.at[idx, "demographics"] = fetch_demographics(geo_id)
            df.at[idx, "value_drivers"] = fetch_value_drivers(geo_id)

        df.at[idx, "pois"] = fetch_pois(lat, lng)
        sleep(0.2)  # be polite
    except Exception as e:
        print(f"Error on row {idx}: {e}")
        continue

# Flatten fields
scores_df = pd.json_normalize(df["location_scores"]).add_prefix("location_scores.")
demo_df = pd.json_normalize(df["demographics"]).add_prefix("demographics.")
vd_df = pd.json_normalize(df["value_drivers"]).add_prefix("value_drivers.")
poi_counts = df["pois"].apply(lambda lst: len(lst) if isinstance(lst, list) else 0)
df["pois.count"] = poi_counts

df_flat = pd.concat([df.drop(columns=["location_scores", "demographics", "value_drivers", "pois"]),
                     scores_df, demo_df, vd_df], axis=1)

# Save the final enriched dataset
df_flat.to_csv("neighborhoods_enriched.csv", index=False)
print("✅ Done: Saved as neighborhoods_enriched.csv")
