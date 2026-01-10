import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import os
import time

# --- LOCAL LOGIC IMPORTS ---
from src.simulation import run_simulation_step, initialize_state

# --- PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="Metropolis Gravity Simulator")

# --- CSS Styling ---
st.markdown("""
<style>
    /* Standard Padding */
    .block-container {
        padding-top: 2rem !important; 
        padding-bottom: 1rem !important;
        max-width: 100% !important;
    }
    
    /* 2. Title Spacing */
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
        border: 1px solid #e0e0e0;
    }
    
    /* 4. Sidebar Status Alert Styling */
    .stAlert {
        padding: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# --- DATA LOADING ---
@st.cache_data
def load_raw_data():
    file_path = "output/simulation_data/clean_places.csv"
    # Fallback logic for safety
    if not os.path.exists(file_path):
        file_path = "output/simulation_data/results.csv"

    if not os.path.exists(file_path):
        st.error(f"❌ File not found. Run `python run_etl.py` first.")
        return pd.DataFrame()
    try:
        df = pd.read_csv(file_path)
        for c in ['latitude', 'longitude', 'population']: 
            df[c] = pd.to_numeric(df[c], errors='coerce')
        df = df.drop_duplicates(subset=['geonameid'])
        return df.dropna(subset=['population'])
    except Exception as e:
        st.error(f"❌ Error reading CSV: {e}")
        return pd.DataFrame()

# --- RENDERER ---
def get_metro_color_map(names):
    # Extended Palette (20+ colors)
    palette = [
        [228,26,28], [55,126,184], [77,175,74], [152,78,163], [255,127,0], 
        [255,255,51], [166,86,40], [247,129,191], [66,245,224], [20,40,120],
        [100,255,100], [255,100,100], [100,100,255], [200,200,50], [50,200,200],
        [200,50,200], [128,0,0], [0,128,128], [128,128,0], [0,0,128], [255,20,147]
    ]
    return {name: palette[i % len(palette)] for i, name in enumerate(sorted(names))}

def render_map(metros, towns):
    color_map = get_metro_color_map(metros['name'].unique())
    
    metros['color'] = metros['name'].apply(lambda x: color_map.get(x, [200,200,200]))
    metros['radius_meters'] = metros['radius_km'] * 1000
    
    captured = towns[towns['metro_id'].notna()].copy()
    captured['color'] = captured['metro_name'].apply(lambda x: color_map.get(x, [200,200,200]))
    
    orphans = towns[towns['metro_id'].isna()].copy()
    orphans['color'] = [[200, 200, 200, 80]] * len(orphans)

    # --- PREPARE CONNECTION LINES ---
    connections = pd.DataFrame()
    if not captured.empty:
        # We need the Metro (Target) coordinates for every captured town
        # Prepare a small lookup DF for metros
        metro_coords = metros[['geonameid', 'latitude', 'longitude']].rename(
            columns={'geonameid': 'metro_id', 'latitude': 'target_lat', 'longitude': 'target_lon'}
        )
        # Merge to get Source (Town) and Target (Metro) in one row
        connections = pd.merge(captured, metro_coords, on='metro_id')
    # ----------------------------------------
    
    # Tooltips
    metros['tooltip_impact'] = "N/A (Center)"
    metros['tooltip_empire'] = metros['name']
    if not captured.empty:
        captured['tooltip_impact'] = captured['impact'].apply(lambda x: f"{x:.4f}")
        captured['tooltip_empire'] = captured['metro_name']
    if not orphans.empty:
        orphans['tooltip_impact'] = "None"
        orphans['tooltip_empire'] = "None"

    layers = [
        # 1. Radius Areas
        pdk.Layer("ScatterplotLayer", data=metros, get_position='[longitude, latitude]', 
                  get_fill_color=[0,0,0,0], get_line_color='color', get_radius='radius_meters', 
                  stroked=True, filled=True, line_width_min_pixels=2, opacity=0.5, pickable=False),
        
        # 2. Connectivity Lines 
        pdk.Layer("LineLayer", data=connections, 
                  get_source_position='[longitude, latitude]', 
                  get_target_position='[target_lon, target_lat]',
                  get_color='color', 
                  get_width=2, 
                  opacity=0.01, # Semi-transparent to avoid clutter
                  pickable=False),

        # 3. Orphans (Always Visible)
        pdk.Layer("ScatterplotLayer", data=orphans, get_position='[longitude, latitude]', 
                  get_color='color', get_radius=2000, pickable=True, opacity=0.6, 
                  stroked=True, line_width_min_pixels=1, get_line_color=[0,0,0,30]),
        
        # 4. Captured Towns
        pdk.Layer("ScatterplotLayer", data=captured, get_position='[longitude, latitude]', 
                  get_color='color', get_radius=3000, pickable=True, opacity=0.8, 
                  stroked=True, line_width_min_pixels=1, get_line_color=[0,0,0,50]),
        
        # 5. Metropolis Centers
        pdk.Layer("ScatterplotLayer", data=metros, get_position='[longitude, latitude]', 
                  get_color='color', get_radius=9000, pickable=True, stroked=True, 
                  filled=True, line_width_min_pixels=2, get_line_color=[0,0,0,255])
    ]
    
    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state=pdk.ViewState(latitude=52.1, longitude=19.0, zoom=5.4),
        layers=layers,
        tooltip={"html": "<b>{name}</b><br/>Pop: {population}<br/>Empire: {tooltip_empire}<br/>Impact: {tooltip_impact}"}
    ), height=580) # Comfort Height

# --- MAIN APP ---
st.title("🏙️ Polish Empire Simulator")

raw_data = load_raw_data()
if raw_data.empty: st.stop()

# --- SIDEBAR ---
st.sidebar.title("🎛️ Simulation Controls")

threshold = st.sidebar.number_input("Metropolis Threshold (population)", value=200000, step=10000)
min_r, max_r = st.sidebar.slider("Radius Range (km)", 1, 200, (10, 90))
status_placeholder = st.sidebar.empty()
run_sim = st.sidebar.button("▶️ START SIMULATION", type="primary")

# --- INITIALIZATION ---
seed_metros, seed_towns = initialize_state(raw_data, threshold, min_r, max_r)
if seed_metros.empty:
    st.error(f"No cities found > {threshold}. Lower threshold.")
    st.stop()

# --- SIMULATION ---
map_placeholder = st.empty()

if run_sim:
    curr_metros, curr_towns = seed_metros.copy(), seed_towns.copy()
    curr_metros['growth_pct'] = 0.0
    stabilized = False # Flag to track completion status
    
    for i in range(1, 20):
        status_placeholder.info(f"🔄 Processing Round {i}...")
        
        # 1. Render Map (Current State)
        with map_placeholder.container():
            render_map(curr_metros, curr_towns)
        
        # 2. Run Physics
        new_captures = run_simulation_step(curr_towns, curr_metros, i, min_r, max_r)
        
        # 3. Check for Stabilization
        if new_captures.empty:
            status_placeholder.success(f"✅ Stabilized at Round {i-1}!")
            stabilized = True
            break
            
        # 4. Apply Updates (Snowball Logic)
        curr_towns.set_index('geonameid', inplace=True)
        new_captures.set_index('geonameid', inplace=True)
        curr_towns.update(new_captures)
        curr_towns.reset_index(inplace=True); new_captures.reset_index(inplace=True)
        
        gains = new_captures.groupby('metro_id')['population'].sum()
        curr_metros.set_index('geonameid', inplace=True)
        curr_metros['current_pop'] += gains.reindex(curr_metros.index).fillna(0)
        curr_metros['added_pop'] += gains.reindex(curr_metros.index).fillna(0)
        curr_metros['added_towns'] += new_captures.groupby('metro_id').size().reindex(curr_metros.index).fillna(0)
        curr_metros.reset_index(inplace=True)
        curr_metros['growth_pct'] = (curr_metros['added_pop'] / curr_metros['population']) * 100
        
        time.sleep(1.0)
    
    # 5. Handle Max Rounds Case (If loop finishes without break)
    if not stabilized:
        status_placeholder.warning("⚠️ Simulation stopped (Max 20 Rounds reached)")

    # 6. Final Stats Render
    with map_placeholder.container():
        render_map(curr_metros, curr_towns)
        
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        
        top_total = curr_metros.sort_values('current_pop', ascending=False).iloc[0]
        top_gain = curr_metros.sort_values('added_pop', ascending=False).iloc[0]
        top_growth = curr_metros.sort_values('growth_pct', ascending=False).iloc[0]
        top_towns = curr_metros.sort_values('added_towns', ascending=False).iloc[0]

        col1.metric("👑 Total Empire Pop", f"{top_total['name']}", f"{top_total['current_pop']:,.0f}")
        col2.metric("📈 Pop Gained", f"{top_gain['name']}", f"+{top_gain['added_pop']:,.0f}")
        col3.metric("🚀 Relative Growth", f"{top_growth['name']}", f"+{top_growth['growth_pct']:.1f}%")
        col4.metric("🏘️ Towns Captured", f"{top_towns['name']}", f"{top_towns['added_towns']:.0f}")

else:
    status_placeholder.info("Ready to Start")
    with map_placeholder.container():
        render_map(seed_metros, seed_towns)