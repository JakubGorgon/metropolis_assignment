import streamlit as st
import pandas as pd
import pydeck as pdk
import glob

# Page Config
st.set_page_config(layout="wide", page_title="Metropolis Growth Simulator")

# Load Data
@st.cache_data
def load_data():
    files = glob.glob("output/simulation_data/*.csv")
    if not files:
        return pd.DataFrame()
    
    df = pd.read_csv(files[0])
    
    # Clean up any potential Spark headers or nulls
    numeric_cols = ['latitude', 'longitude', 'population', 'distance_km', 'impact']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    return df

df = load_data()

st.title("🏙️ Metropolis Gravity Model")
st.markdown("Visualizing the influence of major Polish cities based on population gravity.")

if df.empty:
    st.error("No data found! Run 'python main.py' first.")
    st.stop()

# --- SIDEBAR ---
st.sidebar.header("Filter")
selected_metro = st.sidebar.selectbox(
    "Highlight Metropolis", 
    ["All"] + sorted(df['metro_name'].dropna().unique().tolist())
)

# --- METRICS ---
col1, col2, col3 = st.columns(3)
col1.metric("Total Places", len(df))
col2.metric("Captured Towns", len(df.dropna(subset=['metro_name'])))
col3.metric("Orphans", len(df[df['metro_name'].isna()]))

# --- MAP PREP ---
def get_color(name):
    # If it is an orphan (NaN metro_name), make it Grey
    if pd.isna(name): 
        return [200, 200, 200, 100]
    
    # If selected specific metro, grey out others
    if selected_metro != "All" and name != selected_metro:
         return [200, 200, 200, 50]
         
    return [255, 0, 0, 160] # Red

df['color'] = df['metro_name'].apply(get_color)
df['radius'] = df['metro_name'].apply(lambda x: 500 if pd.isna(x) else 1500)

# --- PYDECK ---
layers = [
    pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position='[longitude, latitude]',
        get_color='color',
        get_radius='radius',
        pickable=True,
        auto_highlight=True
    )
]

st.pydeck_chart(pdk.Deck(
    map_style="mapbox://styles/mapbox/light-v9",
    initial_view_state=pdk.ViewState(
        latitude=52.0,
        longitude=19.0,
        zoom=6,
        pitch=0,
    ),
    layers=layers,
    tooltip={"html": "<b>{name}</b><br/>Pop: {population}<br/>Metro: {metro_name}<br/>Impact: {impact}"}
))