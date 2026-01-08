import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import os

# --- PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="Metropolis Gravity Simulator")

# --- CUSTOM CSS (Safe Compact Layout) ---
st.markdown("""
<style>
    /* 1. Reduce Main Container Padding (Safe Zone) */
    .block-container {
        padding-top: 2rem !important; /* Enough space for the top menu, but tight */
        padding-bottom: 1rem !important;
        max-width: 100% !important;
    }
    
    /* 2. Reset Title Margins (Don't pull it off screen) */
    h1 {
        margin-top: 1rem !important;
        padding-top: 0rem !important;
        margin-bottom: 1rem !important;
    }

    /* 3. Metric Box Styling */
    div[data-testid="metric-container"] {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# --- PHYSICS CONSTANTS ---
METRO_POP_CONSTANT = -1/1443000
METRO_POWER_CONSTANT = -1.4
EARTH_RADIUS_KM = 6371.0 

# --- 1. ROBUST DATA LOADING ---
@st.cache_data
def load_base_data():
    file_path = "output/simulation_data/results.csv"
    
    if not os.path.exists(file_path):
        st.error(f"❌ File not found: `{file_path}`")
        return pd.DataFrame()
        
    try:
        df = pd.read_csv(file_path)
        df['population'] = pd.to_numeric(df['population'], errors='coerce')
        df = df.dropna(subset=['population'])
        
        for c in ['latitude', 'longitude']:
            df[c] = pd.to_numeric(df[c], errors='coerce')

        return df
        
    except Exception as e:
        st.error(f"❌ Error reading CSV: {e}")
        return pd.DataFrame()

# --- 2. PHYSICS ENGINE ---
def calculate_geodesic_distance(lat1, lon1, lat2, lon2):
    lat1, lon1 = np.radians(lat1), np.radians(lon1)
    lat2, lon2 = np.radians(lat2), np.radians(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    return EARTH_RADIUS_KM * c

def run_gravity_model(df, max_radius_km, min_radius_km, metro_threshold):
    # Strip old columns to prevent collisions
    clean_cols = ['geonameid', 'name', 'population', 'latitude', 'longitude']
    available_cols = [c for c in clean_cols if c in df.columns]
    df_clean = df[available_cols].copy()

    metros = df_clean[df_clean['population'] >= metro_threshold].copy()
    towns = df_clean[df_clean['population'] < metro_threshold].copy()
    
    if metros.empty:
        return df, pd.DataFrame(columns=df.columns)
        
    metros['radius_km'] = min_radius_km + (max_radius_km - min_radius_km) * (
        1 - np.exp(METRO_POP_CONSTANT * metros['population'])
    )
    
    towns['key'] = 1
    metros['key'] = 1
    
    metros_renamed = metros[['key', 'name', 'latitude', 'longitude', 'radius_km', 'geonameid']].rename(
        columns={
            'name': 'metro_name', 
            'latitude': 'metro_lat', 
            'longitude': 'metro_lon',
            'radius_km': 'metro_radius',
            'geonameid': 'metro_id'
        }
    )
    
    joined = pd.merge(towns, metros_renamed, on='key').drop('key', axis=1)
    
    joined['distance_km'] = calculate_geodesic_distance(
        joined['latitude'], joined['longitude'],
        joined['metro_lat'], joined['metro_lon']
    )
    
    candidates = joined[joined['distance_km'] <= joined['metro_radius']].copy()
    candidates['impact'] = np.exp(METRO_POWER_CONSTANT * (candidates['distance_km'] / candidates['metro_radius'])).round(6)
    
    if not candidates.empty:
        winners = candidates.loc[candidates.groupby('geonameid')['impact'].idxmax()]
        cols_to_keep = ['geonameid', 'metro_name', 'metro_id', 'distance_km', 'impact']
        winners = winners[cols_to_keep]
    else:
        winners = pd.DataFrame(columns=['geonameid', 'metro_name', 'metro_id', 'distance_km', 'impact'])
    
    final_df = pd.merge(towns, winners, on='geonameid', how='left')
    
    metros['metro_name'] = metros['name']
    metros['metro_id'] = metros['geonameid']
    metros['distance_km'] = 0.0
    metros['impact'] = 1.0
    
    full_result = pd.concat([final_df, metros], ignore_index=True)
    return full_result, metros

# --- 3. UTILS ---
def get_metro_color_map(metro_names):
    palette = [
        [228,26,28], [55,126,184], [77,175,74], [152,78,163], [255,127,0], 
        [255,255,51], [166,86,40], [247,129,191], [153,153,153], [66, 245, 224],
        [245, 66, 135], [99, 245, 66], [48, 25, 52], [120, 120, 20], [20, 40, 120],
        [100, 100, 100], [200, 50, 50], [50, 200, 50], [50, 50, 200]
    ]
    color_map = {}
    for i, name in enumerate(sorted(metro_names)):
        color_map[name] = palette[i % len(palette)]
    return color_map

# --- MAIN APP ---
st.title("🏙️ Polish Empire Simulator")

raw_df = load_base_data()
if raw_df.empty:
    st.stop()

# 2. SIDEBAR CONTROLS
st.sidebar.title("🎛️ Gravity Controls")
st.sidebar.subheader("Physics Parameters")

metro_threshold = st.sidebar.number_input("Metropolis Threshold (Pop)", value=200000, step=10000)
min_radius = st.sidebar.slider("Min Radius (km)", 5.0, 50.0, 10.0)
max_radius = st.sidebar.slider("Max Radius (km)", 50.0, 200.0, 100.0)

# 3. RUN MODEL
sim_df, metros_df = run_gravity_model(raw_df, max_radius, min_radius, metro_threshold)

if metros_df.empty:
    st.warning(f"⚠️ No cities found with population > {metro_threshold}.")
    st.stop()

# 4. VISUALIZATION CONTROLS
st.sidebar.subheader("Visualization")
all_metros = sorted(metros_df['name'].unique())
selected_metros = st.sidebar.multiselect("Highlight Empires", all_metros, default=all_metros)

# 5. PREPARE LAYERS
color_map = get_metro_color_map(all_metros)
metros_data = metros_df.copy()
towns_data = sim_df[sim_df['population'] < metro_threshold].copy()

def get_town_color(row):
    if pd.isna(row['metro_name']): return [200, 200, 200, 80]
    if row['metro_name'] in selected_metros: return color_map.get(row['metro_name'], [200,200,200])
    return [200, 200, 200, 30]

towns_data['color'] = towns_data.apply(get_town_color, axis=1)
metros_data['color'] = metros_data['name'].apply(lambda x: color_map.get(x, [255,0,0]))
metros_data['radius_meters'] = metros_data['radius_km'] * 1000 

active_circles = metros_data[metros_data['name'].isin(selected_metros)]

# Layers
town_layer = pdk.Layer(
    "ScatterplotLayer",
    data=towns_data,
    get_position='[longitude, latitude]',
    get_color='color',
    get_radius=3000,
    pickable=True,
    opacity=0.8,
    stroked=True,
    filled=True,
    line_width_min_pixels=1,
    get_line_color=[0,0,0,50]
)

metro_center_layer = pdk.Layer(
    "ScatterplotLayer",
    data=metros_data,
    get_position='[longitude, latitude]',
    get_color='color',
    get_radius=9000,
    pickable=True,
    stroked=True,
    filled=True,
    line_width_min_pixels=2,
    get_line_color=[0,0,0,255]
)

radius_layer = pdk.Layer(
    "ScatterplotLayer",
    data=active_circles,
    get_position='[longitude, latitude]',
    get_fill_color=[0, 0, 0, 0],
    get_line_color='color',
    get_radius='radius_meters',
    pickable=False,
    stroked=True,
    filled=True,
    line_width_min_pixels=3,
    opacity=0.5
)

# 6. RENDER MAP
view_state = pdk.ViewState(latitude=52.0, longitude=19.0, zoom=5.4, pitch=0)

st.pydeck_chart(pdk.Deck(
    map_style="mapbox://styles/mapbox/light-v9",
    initial_view_state=view_state,
    layers=[radius_layer, town_layer, metro_center_layer],
    tooltip={"html": "<b>{name}</b><br/>Pop: {population}<br/>Empire: {metro_name}<br/>Impact: {impact}"},
), height=570)

# 7. SORTED LEADERBOARD (By Gained Population)
if selected_metros:
    st.subheader("🏆 Empire Leaderboard (Sorted by Gained Population)")
    
    leaderboard = []
    for m in selected_metros:
        # Get the empire rows
        empire = sim_df[sim_df['metro_name'] == m]
        
        # Calculate TOTAL Population (Metro + Towns)
        total_pop = empire['population'].sum()
        
        # Calculate GAINED Population (Total - Metro's own size)
        # We find the row that is the metro itself (distance=0) and subtract it
        metro_self_pop = empire[empire['distance_km'] == 0]['population'].sum()
        gained_pop = total_pop - metro_self_pop
        
        towns_count = len(empire) - 1 # Exclude self
        
        leaderboard.append({
            "name": m,
            "gained_pop": gained_pop,
            "total_pop": total_pop,
            "towns": towns_count
        })
    
    # Sort by GAINED Population Descending
    leaderboard.sort(key=lambda x: x['gained_pop'], reverse=True)
    
    cols = st.columns(min(len(leaderboard), 4))
    
    for idx, stat in enumerate(leaderboard):
        col_idx = idx % 4
        with cols[col_idx]:
            st.metric(
                label=f"#{idx+1} {stat['name']}", 
                value=f"+{stat['gained_pop']:,.0f} ppl", 
                delta=f"{stat['towns']} towns captured"
            )
        if (idx + 1) % 4 == 0 and (idx + 1) < len(leaderboard):
            st.write("---")
            cols = st.columns(4)