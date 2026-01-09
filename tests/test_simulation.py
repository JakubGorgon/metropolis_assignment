import pytest
import pandas as pd
import numpy as np
from src.simulation import initialize_state, run_simulation_step
from src.config import METRO_POP_CONSTANT

# --- FIXTURES ---
@pytest.fixture
def scenario_data():
    """
    Creates a mini-Poland with:
    1. 'Warsaw' (Giant, 1.7M) at (0,0)
    2. 'Pruszkow' (Town, 60k) close to Warsaw (0.1 deg away)
    3. 'Lodz' (Large, 700k) far away (1.0 deg away)
    4. 'OrphanVille' (Tiny) extremely far away (50 deg away)
    """
    raw_data = pd.DataFrame({
        'geonameid': [1, 2, 3, 4],
        'name': ['Warsaw', 'Pruszkow', 'Lodz', 'OrphanVille'],
        'population': [1_700_000, 60_000, 700_000, 100],
        'latitude':  [52.0, 52.0, 52.0, 0.0],
        'longitude': [21.0, 21.15, 20.0, 0.0] 
        # Note: 0.15 deg longitude diff at lat 52 is roughly ~10km
    })
    return raw_data

# --- TESTS ---

def test_initialization_split(scenario_data):
    """Test 1: Does the threshold correctly separate Kings from Peasants?"""
    threshold = 200_000
    metros, towns = initialize_state(scenario_data, threshold, 10, 100)
    
    # Warsaw and Lodz should be Metros
    assert len(metros) == 2
    assert "Warsaw" in metros['name'].values
    assert "Lodz" in metros['name'].values
    
    # Pruszkow and OrphanVille should be Towns
    assert len(towns) == 2
    assert "Pruszkow" in towns['name'].values

def test_gravity_capture_logic(scenario_data):
    """Test 2: Does Warsaw successfully eat Pruszkow?"""
    threshold = 200_000
    metros, towns = initialize_state(scenario_data, threshold, 10, 90)
    
    # Run 1 step of simulation
    winners = run_simulation_step(towns, metros, iteration=1, min_r=10, max_r=90)
    
    # Pruszkow (ID 2) is close (~10km) to Warsaw. It should be captured.
    pruszkow_capture = winners[winners['geonameid'] == 2]
    
    assert not pruszkow_capture.empty
    assert pruszkow_capture.iloc[0]['metro_name'] == "Warsaw"
    
    # Impact should be < 1.0 but > 0.0
    impact = pruszkow_capture.iloc[0]['impact']
    assert 0.0 < impact < 1.0

def test_orphan_survival(scenario_data):
    """Test 3: Does OrphanVille stay free because it's too far?"""
    metros, towns = initialize_state(scenario_data, 200_000, 10, 90)
    
    winners = run_simulation_step(towns, metros, iteration=1, min_r=10, max_r=90)
    
    # OrphanVille (ID 4) is thousands of km away. It should NOT appear in winners.
    orphan_capture = winners[winners['geonameid'] == 4]
    assert orphan_capture.empty

def test_snowball_effect_math():
    """
    Test 4: THE SNOWBALL
    Verify mathematically that adding population INCREASES the radius.
    """
    # Create a dummy city
    df = pd.DataFrame({'current_pop': [200_000]})
    
    # Calculate Radius A
    radius_a = 10 + 90 * (1 - np.exp(METRO_POP_CONSTANT * df.loc[0, 'current_pop']))
    
    # Snowball: Double the population
    df.loc[0, 'current_pop'] = 400_000
    
    # Calculate Radius B
    radius_b = 10 + 90 * (1 - np.exp(METRO_POP_CONSTANT * df.loc[0, 'current_pop']))
    
    # Radius B MUST be significantly larger than Radius A
    assert radius_b > radius_a + 5.0 # It should grow by at least 5km with that pop jump

def test_competition_winner_takes_all():
    """
    Test 5: COMPETITION
    Town sits exactly between Giant (Far) and Dwarf (Near).
    Dwarf should win if it's close enough.
    """
    metros = pd.DataFrame({
        'geonameid': [1, 2],
        'name': ['Giant', 'Dwarf'],
        'current_pop': [1_000_000, 250_000],
        'latitude': [52.0, 52.0],
        'longitude': [21.5, 21.05], # Giant is at 0.5 distance, Dwarf at 0.05
        'radius_km': [100.0, 50.0]  # Hardcode radii to ensure overlap
    })
    
    # Town is at 21.00 (Very close to Dwarf)
    towns = pd.DataFrame({
        'geonameid': [99], 
        'metro_id': [np.nan],
        'latitude': [52.0], 
        'longitude': [21.0] 
    })
    
    winners = run_simulation_step(towns, metros, 1, 10, 90)
    
    # The Dwarf is 10x closer, so despite being 4x smaller, it should win
    assert winners.iloc[0]['metro_name'] == "Dwarf"