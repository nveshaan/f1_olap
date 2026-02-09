import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, Tuple

def get_session_corner_analysis(db_path: str, year: int, event: str, session_type: str) -> Tuple[Dict, Dict]:
    """
    Perform OLAP analysis comparing average corner speeds between two drivers for a session.
    
    Args:
        db_path: Path to SQLite database
        year: Year of the event
        event: Event name (e.g., 'Monaco')
        session_type: Session type (e.g., 'R' for Race)
    
    Returns:
        Tuple of (avg_corner_speeds dict, overall_avg_speeds dict)
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Find the session
    cursor.execute("""
        SELECT s.id, c.id as circuit_id
        FROM sessions s
        JOIN circuits c ON s.circuit_id = c.id
        WHERE strftime('%Y', s.date) = ?
        AND s.event_name LIKE ?
        AND s.session_name LIKE ?
    """, (str(year), f"%{event}%", f"%{session_type}%"))
    
    session = cursor.fetchone()
    if not session:
        raise ValueError(f"Session not found: {year} {event} {session_type}")
    
    session_id = session['id']
    circuit_id = session['circuit_id']
    
    # Get driver abbreviations and their info
    cursor.execute("""
        SELECT id, abbrevation FROM drivers 
        WHERE abbrevation IN ('VER', 'NOR')
    """)
    drivers = {row['abbrevation']: row['id'] for row in cursor.fetchall()}
    
    if len(drivers) < 2:
        raise ValueError("Could not find both VER and NOR drivers")
    
    ver_id = drivers['VER']
    nor_id = drivers['NOR']
    
    # Get fastest lap for each driver
    cursor.execute("""
        SELECT id, lap_number FROM laps
        WHERE session_id = ? AND driver_id = ?
        ORDER BY lap_time ASC
        LIMIT 1
    """, (session_id, ver_id))
    ver_lap = cursor.fetchone()
    
    cursor.execute("""
        SELECT id, lap_number FROM laps
        WHERE session_id = ? AND driver_id = ?
        ORDER BY lap_time ASC
        LIMIT 1
    """, (session_id, nor_id))
    nor_lap = cursor.fetchone()
    
    if not ver_lap or not nor_lap:
        raise ValueError("Could not find fastest laps for one or both drivers")
    
    ver_lap_id = ver_lap['id']
    nor_lap_id = nor_lap['id']
    
    # Get corners for the circuit
    cursor.execute("""
        SELECT number, distance FROM corners
        WHERE circuit_id = ?
        ORDER BY distance ASC
    """, (circuit_id,))
    
    corners = cursor.fetchall()
    
    # Get all telemetry for both drivers' fastest laps
    ver_telemetry = pd.read_sql("""
        SELECT speed, distance FROM telemetry
        WHERE lap_id = ?
        ORDER BY distance ASC
    """, conn, params=(ver_lap_id,))
    
    nor_telemetry = pd.read_sql("""
        SELECT speed, distance FROM telemetry
        WHERE lap_id = ?
        ORDER BY distance ASC
    """, conn, params=(nor_lap_id,))
    
    # Calculate average speeds per corner
    avg_corner_speeds = {}
    distance_threshold = 20
    
    for corner in corners:
        corner_num = corner['number']
        corner_dist = corner['distance']
        
        # Filter telemetry data within distance range
        ver_segment = ver_telemetry[
            (ver_telemetry['distance'] >= corner_dist - distance_threshold) &
            (ver_telemetry['distance'] <= corner_dist + distance_threshold)
        ]
        
        nor_segment = nor_telemetry[
            (nor_telemetry['distance'] >= corner_dist - distance_threshold) &
            (nor_telemetry['distance'] <= corner_dist + distance_threshold)
        ]
        
        avg_corner_speeds[corner_num] = {
            'VER': ver_segment['speed'].mean() if len(ver_segment) > 0 else None,
            'NOR': nor_segment['speed'].mean() if len(nor_segment) > 0 else None
        }
    
    # Calculate overall average speeds
    overall_avg_speeds = {
        'VER': ver_telemetry['speed'].mean() if len(ver_telemetry) > 0 else None,
        'NOR': nor_telemetry['speed'].mean() if len(nor_telemetry) > 0 else None
    }
    
    conn.close()
    
    return avg_corner_speeds, overall_avg_speeds


def plot_corner_speed_comparison(avg_corner_speeds: Dict, overall_avg_speeds: Dict):
    """Plot average speeds at each corner for two drivers."""
    
    print(f"Avg Speed VER: {overall_avg_speeds['VER']}")
    print(f"Avg Speed NOR: {overall_avg_speeds['NOR']}")
    
    corners = sorted(avg_corner_speeds.keys())
    ver_speeds = [avg_corner_speeds[c]['VER'] for c in corners]
    nor_speeds = [avg_corner_speeds[c]['NOR'] for c in corners]
    
    plt.figure(figsize=(12, 6))
    plt.plot(corners, ver_speeds, label='VER', marker='o', color='red')
    plt.plot(corners, nor_speeds, label='NOR', marker='o', color='blue')
    plt.xlabel('Corner Number')
    plt.ylabel('Average Speed (km/h)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # Example usage
    avg_corner_speeds, overall_avg_speeds = get_session_corner_analysis(
        db_path='f1.db',
        year=2025,
        event='Australia',
        session_type='R'
    )
    
    plot_corner_speed_comparison(avg_corner_speeds, overall_avg_speeds)
