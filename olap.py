import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, Tuple, List, Optional

class F1OLAPAnalysis:
    """F1 OLAP Analysis - Multidimensional data analysis on F1 racing data"""
    
    def __init__(self, db_path: str):
        """Initialize OLAP analysis with database connection"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
    
    def __del__(self):
        """Close database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()
    
    # ============== SLICE OPERATION ==============
    # Filtering on a single dimension
    
    def slice_by_driver(self, driver_abbrev: str, year: int = None) -> pd.DataFrame:
        """
        SLICE: Get all sessions for a specific driver
        
        Args:
            driver_abbrev: Driver abbreviation (e.g., 'VER', 'NOR')
            year: Optional year filter
        
        Returns:
            DataFrame with driver's performance across sessions
        """
        query = """
            SELECT 
                s.event_name,
                s.session_name,
                s.date,
                d.abbrevation as driver,
                t.name as team,
                r.position,
                r.points,
                r.laps,
                r.status
            FROM results r
            JOIN sessions s ON r.session_id = s.id
            JOIN drivers d ON r.driver_id = d.id
            JOIN teams t ON r.team_id = t.id
            WHERE d.abbrevation = ?
        """
        params = [driver_abbrev]
        
        if year:
            query += " AND strftime('%Y', s.date) = ?"
            params.append(str(year))
        
        query += " ORDER BY s.date DESC"
        
        return pd.read_sql(query, self.conn, params=params)
    
    def slice_by_circuit(self, circuit_name: str) -> pd.DataFrame:
        """
        SLICE: Get all sessions at a specific circuit
        
        Args:
            circuit_name: Circuit name
        
        Returns:
            DataFrame with all races at that circuit
        """
        query = """
            SELECT 
                s.event_name,
                s.session_name,
                s.date,
                d.abbrevation as driver,
                t.name as team,
                r.position,
                r.points,
                c.name as circuit
            FROM results r
            JOIN sessions s ON r.session_id = s.id
            JOIN drivers d ON r.driver_id = d.id
            JOIN teams t ON r.team_id = t.id
            JOIN circuits c ON s.circuit_id = c.id
            WHERE c.name LIKE ?
            ORDER BY s.date DESC
        """
        return pd.read_sql(query, self.conn, params=[f"%{circuit_name}%"])
    
    def slice_by_year(self, year: int) -> pd.DataFrame:
        """
        SLICE: Get all results for a specific year
        
        Args:
            year: Year to filter
        
        Returns:
            DataFrame with all results in that year
        """
        query = """
            SELECT 
                s.event_name,
                s.session_name,
                s.date,
                d.abbrevation as driver,
                t.name as team,
                r.position,
                r.points
            FROM results r
            JOIN sessions s ON r.session_id = s.id
            JOIN drivers d ON r.driver_id = d.id
            JOIN teams t ON r.team_id = t.id
            WHERE strftime('%Y', s.date) = ?
            ORDER BY s.date DESC
        """
        return pd.read_sql(query, self.conn, params=[str(year)])
    
    # ============== DICE OPERATION ==============
    # Filtering on multiple dimensions
    
    def dice_driver_circuit_year(self, driver_abbrev: str, circuit_name: str, year: int) -> pd.DataFrame:
        """
        DICE: Filter by driver, circuit, and year simultaneously
        
        Args:
            driver_abbrev: Driver abbreviation
            circuit_name: Circuit name
            year: Year
        
        Returns:
            DataFrame with specific driver's performance at specific circuit in specific year
        """
        query = """
            SELECT 
                s.event_name,
                s.session_name,
                s.date,
                d.abbrevation as driver,
                t.name as team,
                r.position,
                r.grid_position,
                r.points,
                c.name as circuit
            FROM results r
            JOIN sessions s ON r.session_id = s.id
            JOIN drivers d ON r.driver_id = d.id
            JOIN teams t ON r.team_id = t.id
            JOIN circuits c ON s.circuit_id = c.id
            WHERE d.abbrevation = ?
            AND c.name LIKE ?
            AND strftime('%Y', s.date) = ?
            ORDER BY s.date DESC
        """
        return pd.read_sql(query, self.conn, params=[driver_abbrev, f"%{circuit_name}%", str(year)])
    
    def dice_drivers_session(self, driver_abbrevs: List[str], event_name: str, session_type: str) -> pd.DataFrame:
        """
        DICE: Compare multiple drivers in a specific session
        
        Args:
            driver_abbrevs: List of driver abbreviations
            event_name: Event name (e.g., 'Monaco')
            session_type: Session type (e.g., 'R', 'Q')
        
        Returns:
            DataFrame comparing drivers in specific session
        """
        placeholders = ','.join(['?' for _ in driver_abbrevs])
        query = f"""
            SELECT 
                s.event_name,
                s.session_name,
                d.abbrevation as driver,
                t.name as team,
                r.position,
                r.grid_position,
                r.time,
                r.laps,
                r.status
            FROM results r
            JOIN sessions s ON r.session_id = s.id
            JOIN drivers d ON r.driver_id = d.id
            JOIN teams t ON r.team_id = t.id
            WHERE d.abbrevation IN ({placeholders})
            AND s.event_name LIKE ?
            AND s.session_name LIKE ?
            ORDER BY r.position ASC
        """
        params = driver_abbrevs + [f"%{event_name}%", f"%{session_type}%"]
        return pd.read_sql(query, self.conn, params=params)
    
    # ============== DRILL DOWN OPERATION ==============
    # Moving to lower levels of detail
    
    def drill_down_to_laps(self, driver_abbrev: str, event_name: str, session_type: str) -> pd.DataFrame:
        """
        DRILL DOWN: From session results to individual lap data
        
        Args:
            driver_abbrev: Driver abbreviation
            event_name: Event name
            session_type: Session type
        
        Returns:
            DataFrame with detailed lap-by-lap data
        """
        query = """
            SELECT 
                s.event_name,
                s.session_name,
                d.abbrevation as driver,
                l.lap_number,
                l.lap_time,
                l.personal_best,
                l.sector1_time,
                l.sector2_time,
                l.sector3_time,
                l.compound,
                l.tyre_life,
                l.position
            FROM laps l
            JOIN sessions s ON l.session_id = s.id
            JOIN drivers d ON l.driver_id = d.id
            WHERE d.abbrevation = ?
            AND s.event_name LIKE ?
            AND s.session_name LIKE ?
            ORDER BY l.lap_number ASC
        """
        return pd.read_sql(query, self.conn, params=[driver_abbrev, f"%{event_name}%", f"%{session_type}%"])
    
    def drill_down_to_telemetry(self, driver_abbrev: str, event_name: str, session_type: str, lap_number: int) -> pd.DataFrame:
        """
        DRILL DOWN: From lap data to telemetry (speed, throttle, brake, etc.)
        
        Args:
            driver_abbrev: Driver abbreviation
            event_name: Event name
            session_type: Session type
            lap_number: Specific lap number
        
        Returns:
            DataFrame with detailed telemetry data
        """
        query = """
            SELECT 
                t.time,
                t.distance,
                t.speed,
                t.rpm,
                t.ngear,
                t.throttle,
                t.brake,
                t.drs,
                t.X,
                t.Y,
                t.Z
            FROM telemetry t
            JOIN laps l ON t.lap_id = l.id
            JOIN sessions s ON l.session_id = s.id
            JOIN drivers d ON l.driver_id = d.id
            WHERE d.abbrevation = ?
            AND s.event_name LIKE ?
            AND s.session_name LIKE ?
            AND l.lap_number = ?
            ORDER BY t.distance ASC
        """
        return pd.read_sql(query, self.conn, params=[driver_abbrev, f"%{event_name}%", f"%{session_type}%", lap_number])
    
    # ============== ROLL UP OPERATION ==============
    # Aggregating data to higher levels
    
    def roll_up_season_standings(self, year: int) -> pd.DataFrame:
        """
        ROLL UP: Aggregate race results to season standings
        
        Args:
            year: Season year
        
        Returns:
            DataFrame with championship standings
        """
        query = """
            SELECT 
                d.abbrevation as driver,
                d.first_name || ' ' || d.last_name as full_name,
                t.name as team,
                COUNT(DISTINCT s.id) as races,
                SUM(r.points) as total_points,
                COUNT(CASE WHEN r.position = 1 THEN 1 END) as wins,
                COUNT(CASE WHEN r.position = 2 THEN 1 END) as podiums,
                AVG(CAST(r.position AS FLOAT)) as avg_position
            FROM results r
            JOIN sessions s ON r.session_id = s.id
            JOIN drivers d ON r.driver_id = d.id
            JOIN teams t ON r.team_id = t.id
            WHERE s.session_name LIKE '%R%'
            AND strftime('%Y', s.date) = ?
            GROUP BY d.id, t.id
            ORDER BY total_points DESC, wins DESC
        """
        return pd.read_sql(query, self.conn, params=[str(year)])
    
    def roll_up_team_standings(self, year: int) -> pd.DataFrame:
        """
        ROLL UP: Aggregate driver results to team standings
        
        Args:
            year: Season year
        
        Returns:
            DataFrame with team championship standings
        """
        query = """
            SELECT 
                t.name as team,
                COUNT(DISTINCT s.id) as races,
                SUM(r.points) as total_points,
                COUNT(CASE WHEN r.position = 1 THEN 1 END) as wins,
                AVG(CAST(r.position AS FLOAT)) as avg_position
            FROM results r
            JOIN sessions s ON r.session_id = s.id
            JOIN teams t ON r.team_id = t.id
            WHERE s.session_name LIKE '%R%'
            AND strftime('%Y', s.date) = ?
            GROUP BY t.id
            ORDER BY total_points DESC, wins DESC
        """
        return pd.read_sql(query, self.conn, params=[str(year)])
    
    def roll_up_circuit_performance(self, driver_abbrev: str) -> pd.DataFrame:
        """
        ROLL UP: Aggregate lap data to circuit-level performance
        
        Args:
            driver_abbrev: Driver abbreviation
        
        Returns:
            DataFrame with performance stats per circuit
        """
        query = """
            SELECT 
                c.name as circuit,
                COUNT(DISTINCT s.id) as visits,
                AVG(CAST(r.position AS FLOAT)) as avg_finish,
                COUNT(CASE WHEN r.position = 1 THEN 1 END) as wins,
                SUM(r.points) as total_points
            FROM results r
            JOIN sessions s ON r.session_id = s.id
            JOIN circuits c ON s.circuit_id = c.id
            JOIN drivers d ON r.driver_id = d.id
            WHERE d.abbrevation = ?
            AND s.session_name LIKE '%R%'
            GROUP BY c.id
            ORDER BY total_points DESC
        """
        return pd.read_sql(query, self.conn, params=[driver_abbrev])
    
    # ============== PIVOT OPERATION ==============
    # Rotating data to different perspectives
    
    def pivot_drivers_by_session(self, event_name: str, year: int = None) -> pd.DataFrame:
        """
        PIVOT: Show drivers (rows) vs sessions (columns) matrix
        
        Args:
            event_name: Event name
            year: Optional year filter
        
        Returns:
            Pivoted DataFrame with drivers and sessions
        """
        data = self.slice_by_circuit(event_name)
        if year:
            data = data[data['date'].str.contains(str(year))]
        
        pivot = data.pivot_table(
            index='driver',
            columns='session_name',
            values='position',
            aggfunc='first'
        )
        return pivot
    
    def pivot_points_by_driver_and_year(self) -> pd.DataFrame:
        """
        PIVOT: Show drivers (rows) vs years (columns) with points
        
        Returns:
            Pivoted DataFrame with drivers and years
        """
        query = """
            SELECT 
                d.abbrevation as driver,
                strftime('%Y', s.date) as year,
                SUM(r.points) as points
            FROM results r
            JOIN sessions s ON r.session_id = s.id
            JOIN drivers d ON r.driver_id = d.id
            WHERE s.session_name LIKE '%R%'
            GROUP BY d.id, year
        """
        data = pd.read_sql(query, self.conn)
        pivot = data.pivot_table(
            index='driver',
            columns='year',
            values='points',
            fill_value=0
        )
        return pivot
    
    def pivot_avg_speed_corner_comparison(self, drivers: List[str], event_name: str, session_type: str) -> pd.DataFrame:
        """
        PIVOT: Compare average corner speeds across drivers
        
        Args:
            drivers: List of driver abbreviations
            event_name: Event name
            session_type: Session type
        
        Returns:
            Pivoted DataFrame with corners (rows) and drivers (columns)
        """
        query = """
            SELECT 
                cor.number as corner,
                d.abbrevation as driver,
                AVG(t.speed) as avg_speed
            FROM telemetry t
            JOIN laps l ON t.lap_id = l.id
            JOIN sessions s ON l.session_id = s.id
            JOIN drivers d ON l.driver_id = d.id
            JOIN corners cor ON s.circuit_id = cor.circuit_id
            WHERE d.abbrevation IN ({})
            AND s.event_name LIKE ?
            AND s.session_name LIKE ?
            AND t.distance BETWEEN cor.distance - 20 AND cor.distance + 20
            GROUP BY cor.id, d.id
        """.format(','.join(['?' for _ in drivers]))
        
        params = drivers + [f"%{event_name}%", f"%{session_type}%"]
        data = pd.read_sql(query, self.conn, params=params)
        
        if data.empty:
            return pd.DataFrame()
        
        pivot = data.pivot_table(
            index='corner',
            columns='driver',
            values='avg_speed'
        )
        return pivot
    
    # ============== VISUALIZATION ==============
    
    def visualize_season_standings(self, year: int):
        """Visualize championship standings"""
        df = self.roll_up_season_standings(year)
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(f'F1 {year} Season Analysis', fontsize=16)
        
        # Points
        axes[0, 0].barh(df['driver'], df['total_points'], color='steelblue')
        axes[0, 0].set_xlabel('Points')
        axes[0, 0].set_title('Championship Points')
        
        # Wins
        axes[0, 1].barh(df['driver'], df['wins'], color='gold')
        axes[0, 1].set_xlabel('Wins')
        axes[0, 1].set_title('Race Wins')
        
        # Average Position
        axes[1, 0].barh(df['driver'], df['avg_position'], color='lightcoral')
        axes[1, 0].set_xlabel('Average Finish Position')
        axes[1, 0].set_title('Avg Position (Lower is Better)')
        axes[1, 0].invert_xaxis()
        
        # Podiums
        axes[1, 1].barh(df['driver'], df['podiums'], color='mediumseagreen')
        axes[1, 1].set_xlabel('Podiums')
        axes[1, 1].set_title('Podium Finishes')
        
        plt.tight_layout()
        plt.show()
    
    def visualize_corner_speeds_pivot(self, pivot_df: pd.DataFrame):
        """Visualize corner speed comparison"""
        plt.figure(figsize=(12, 6))
        for driver in pivot_df.columns:
            plt.plot(pivot_df.index, pivot_df[driver], marker='o', label=driver, linewidth=2)
        
        plt.xlabel('Corner Number')
        plt.ylabel('Average Speed (km/h)')
        plt.title('Corner Speed Comparison')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    # Initialize OLAP analysis
    olap = F1OLAPAnalysis('f1.db')
    
    print("=" * 60)
    print("F1 OLAP ANALYSIS - Available Operations")
    print("=" * 60)
    
    # ========== SLICE OPERATIONS ==========
    print("\n[SLICE] Driver Performance (2025)")
    df = olap.slice_by_driver('VER', 2025)
    print(df[['event_name', 'session_name', 'position', 'points']].head(10))
    
    print("\n[SLICE] All Races at Monaco")
    df = olap.slice_by_circuit('Monaco')
    print(df[['date', 'driver', 'position', 'points']].head(10))
    
    # ========== DICE OPERATIONS ==========
    print("\n[DICE] VER at Monaco 2025")
    df = olap.dice_driver_circuit_year('VER', 'Monaco', 2025)
    print(df)
    
    print("\n[DICE] VER vs NOR at 2025 Monaco Race")
    df = olap.dice_drivers_session(['VER', 'NOR'], 'Monaco', 'R')
    print(df)
    
    # ========== DRILL DOWN OPERATIONS ==========
    print("\n[DRILL DOWN] VER Laps at Monaco 2025 Race")
    df = olap.drill_down_to_laps('VER', 'Monaco', 'R')
    print(df[['lap_number', 'lap_time', 'compound', 'personal_best']].head(10))
    
    print("\n[DRILL DOWN] VER Telemetry - Lap 1 at Monaco 2025 Race")
    df = olap.drill_down_to_telemetry('VER', 'Monaco', 'R', 1)
    print(df[['distance', 'speed', 'throttle', 'brake']].head(20))
    
    # ========== ROLL UP OPERATIONS ==========
    print("\n[ROLL UP] 2025 Championship Standings")
    df = olap.roll_up_season_standings(2025)
    print(df.head(10))
    
    print("\n[ROLL UP] 2025 Team Standings")
    df = olap.roll_up_team_standings(2025)
    print(df)
    
    print("\n[ROLL UP] VER Performance by Circuit")
    df = olap.roll_up_circuit_performance('VER')
    print(df.head(10))
    
    # ========== PIVOT OPERATIONS ==========
    print("\n[PIVOT] Drivers vs Sessions at Monaco")
    df = olap.pivot_drivers_by_session('Monaco', 2025)
    print(df)
    
    print("\n[PIVOT] Season Points by Driver and Year")
    df = olap.pivot_points_by_driver_and_year()
    print(df.head(10))
    
    print("\n[PIVOT] Corner Speeds: VER vs NOR at Monaco")
    df = olap.pivot_avg_speed_corner_comparison(['VER', 'NOR'], 'Monaco', 'R')
    print(df)
    
    # ========== VISUALIZATIONS ==========
    print("\nGenerating visualizations...")
    olap.visualize_season_standings(2025)
    
    if not olap.pivot_avg_speed_corner_comparison(['VER', 'NOR'], 'Monaco', 'R').empty:
        olap.visualize_corner_speeds_pivot(olap.pivot_avg_speed_corner_comparison(['VER', 'NOR'], 'Monaco', 'R'))
