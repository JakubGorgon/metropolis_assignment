import numpy as np
import pandas as pd
from src.config import METRO_POP_CONSTANT, METRO_POWER_CONSTANT, EARTH_RADIUS_KM

def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Vectorized Haversine Formula.
    Expects inputs in degrees. Returns km.
    """
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    return EARTH_RADIUS_KM * c

def initialize_state(raw_df, threshold, min_r, max_r):
    """
    Splits raw data into Metros and Towns based on threshold.
    """
    metros = raw_df[raw_df['population'] >= threshold].copy()
    towns = raw_df[raw_df['population'] < threshold].copy()
    
    metros['current_pop'] = metros['population']
    metros['added_pop'] = 0
    metros['added_towns'] = 0
    metros['radius_km'] = min_r + max_r * (1 - np.exp(METRO_POP_CONSTANT * metros['current_pop']))
    
    towns['metro_id'] = np.nan
    towns['metro_name'] = None
    towns['distance_km'] = np.nan
    towns['impact'] = np.nan
    towns['iteration_captured'] = np.nan
    
    return metros, towns

def run_simulation_step(towns, metros, iteration, min_r, max_r):
    """
    Core Physics Logic - Optimized with Numpy Broadcasting.
    1. Updates Metro Radius based on current size.
    2. Finds available towns within range.
    3. Calculates Impact score.
    4. Determines winners.
    """
    # 1. Update Radius (Vectorized)
    metros['radius_km'] = min_r + max_r * (
        1 - np.exp(METRO_POP_CONSTANT * metros['current_pop'])
    )
    
    # 2. Filter Orphans
    available = towns[towns['metro_id'].isna()].copy()
    if available.empty: return pd.DataFrame()
    
    # --- OPTIMIZATION START: Numpy Broadcasting instead of Cross Join ---
    
    # Prepare Coordinates (Radians for Haversine)
    # Shape: (N_Towns, 1, 2) vs (1, M_Metros, 2)
    town_rad = np.radians(available[['latitude', 'longitude']].to_numpy())[:, np.newaxis, :]
    metro_rad = np.radians(metros[['latitude', 'longitude']].to_numpy())[np.newaxis, :, :]
    
    # Unpack lat/lon
    lat1, lon1 = town_rad[..., 0], town_rad[..., 1]
    lat2, lon2 = metro_rad[..., 0], metro_rad[..., 1]
    
    # Calculate Distances (Vectorized Haversine)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    dist_matrix = EARTH_RADIUS_KM * c  # Shape: (N, M)
    
    # Metro Radii Broadcast
    radii = metros['radius_km'].to_numpy()[np.newaxis, :] # Shape: (1, M)
    
    # Mask: Inside Radius?
    mask = dist_matrix <= radii
    
    # Calculate Impact Matrix (Zero where outside radius)
    # impact = exp(C * dist / radius)
    # We add a small epsilon to radius to avoid division by zero (unlikely but safe)
    with np.errstate(divide='ignore', invalid='ignore'):
        impact_matrix = np.exp(METRO_POWER_CONSTANT * (dist_matrix / radii))
        impact_matrix[~mask] = 0.0 # Strict Cutoff
        
    # Find Best Metro per Town
    best_metro_idx = np.argmax(impact_matrix, axis=1) # Index of best metro for each town
    max_impact = np.max(impact_matrix, axis=1)        # Value of best impact
    
    # Filter for towns that actually found a metro (impact > 0)
    captured_mask = max_impact > 0
    
    if not np.any(captured_mask):
        return pd.DataFrame()
        
    # Construct Winners DataFrame
    winners_idx = available.index[captured_mask]
    best_metros_iloc = best_metro_idx[captured_mask]
    
    # Get Metro details using iloc
    capturing_metros = metros.iloc[best_metros_iloc]
    
    winners = available.loc[winners_idx].copy()
    winners['metro_id'] = capturing_metros['geonameid'].values
    winners['metro_name'] = capturing_metros['name'].values
    winners['distance_km'] = dist_matrix[captured_mask, best_metros_iloc]
    winners['impact'] = max_impact[captured_mask]
    winners['iteration_captured'] = iteration
    
    return winners

    # --- OPTIMIZATION END ---