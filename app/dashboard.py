import gradio as gr
import plotly.express as px
import pandas as pd
from db_manager import get_connection
from cubes import setup_cubes
import time
from sklearn.cluster import KMeans
from mlxtend.frequent_patterns import fpgrowth, association_rules
from mlxtend.preprocessing import TransactionEncoder
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
import os
import joblib

con = None
MODELS_DIR = "assets/models"
os.makedirs(MODELS_DIR, exist_ok=True)

def connect_db(use_local, local_db_path):
    global con
    try:
        if use_local and local_db_path:
            con = get_connection(local_db_path)
        else:
            con = get_connection()
        setup_cubes(con)
        return "✅ Connected successfully to DB."
    except Exception as e:
        return f"❌ Connection Error: {str(e)}"

# Define metadata for the dynamic cubes allowing slicing, dicing, and pivoting
CUBE_CONFIG = {
    "Results Cube": {
        "fact": "fact_race_results f",
        "joins": "JOIN dim_drivers d ON f.driver_key = d.driver_key JOIN dim_teams t ON f.team_key = t.team_key JOIN dim_sessions s ON f.session_key = s.session_key JOIN dim_circuits c ON f.circuit_key = c.circuit_key",
        "dimensions": {
            "Driver": "d.name",
            "Team": "t.name",
            "Session": "s.session_name",
            "Event": "s.event_name",
            "Circuit": "c.name",
            "Year": "EXTRACT(YEAR FROM s.date::TIMESTAMP)"
        },
        "measures": ["points_scored", "position_gain", "laps_completed"]
    },
    "Lap Strategy Cube": {
        "fact": "fact_laps f",
        "joins": "JOIN dim_drivers d ON f.driver_key = d.driver_key JOIN dim_sessions s ON f.session_key = s.session_key",
        "dimensions": {
            "Driver": "d.name",
            "Session": "s.session_name",
            "Compound": "f.compound",
            "Is Fresh Tyre": "f.fresh_tyre",
            "Lap Number": "f.lap_number",
            "Year": "EXTRACT(YEAR FROM s.date::TIMESTAMP)"
        },
        "measures": ["tyre_life", "lap_time_sec", "s1_time", "s2_time", "s3_time"] # ms metrics
    },
    "Telemetry Cube": {
        "fact": "fact_telemetry f",
        "joins": "JOIN fact_laps l ON f.lap_key = l.lap_key JOIN dim_drivers d ON l.driver_key = d.driver_key JOIN dim_sessions s ON l.session_key = s.session_key",
        "dimensions": {
            "Driver": "d.name",
            "Session": "s.session_name",
            "DRS Status": "f.drs_status",
            "Year": "EXTRACT(YEAR FROM s.date::TIMESTAMP)"
        },
        "measures": ["speed", "rpm", "throttle_pct"]
    }
}

def update_cube_options(cube_name):
    """Dynamically updates the dropdown options based on the selected cube."""
    cfg = CUBE_CONFIG[cube_name]
    dim_choices = list(cfg["dimensions"].keys())
    measure_choices = cfg["measures"]
    
    return [
        gr.Dropdown(choices=dim_choices, value=dim_choices[0]), # X-Axis
        gr.Dropdown(choices=measure_choices, value=measure_choices[0]), # Y-Axis
        gr.Dropdown(choices=["None"] + dim_choices, value="None"), # Pivot By
        gr.Dropdown(choices=["None"] + dim_choices, value="None"), # Slice Dimension
        gr.Dropdown(choices=["Bar", "Line", "Scatter", "Pie"], value="Bar"),
        gr.Textbox(value="") # Empty slice text
    ]

def evaluate_olap_query(cube_name, x_axis, y_axis, pivot_col, slice_dim, slice_val, agg_func, chart_type):
    global con
    if con is None:
        return None, None, "❌ Database not connected!"
        
    cfg = CUBE_CONFIG[cube_name]
    x_sql = cfg["dimensions"][x_axis]
    y_sql = y_axis
    
    select_cols = [f"{x_sql} as \"{x_axis}\"", f"{agg_func}({y_sql}) as \"{y_axis}\""]
    group_cols = [f"{x_sql}"]
    
    if pivot_col != "None":
        pivot_sql = cfg["dimensions"][pivot_col]
        select_cols.append(f"{pivot_sql} as \"{pivot_col}\"")
        group_cols.append(f"{pivot_sql}")
        
    query = f"SELECT {', '.join(select_cols)} FROM {cfg['fact']} {cfg['joins']}"
    
    if slice_dim != "None" and slice_val.strip() != "":
        slice_sql = cfg["dimensions"][slice_dim]
        safe_vals = [v.strip().replace("'", "''") for v in slice_val.split(",")]
        safe_vals_str = ", ".join(f"'{v}'" for v in safe_vals)
        query += f" WHERE {slice_sql} IN ({safe_vals_str})"
        
    query += f" GROUP BY {', '.join(group_cols)} ORDER BY \"{y_axis}\" DESC LIMIT 100"
    
    # ------------------
    # Query 2 for Podiums & Finishing position (if Results Cube is selected)
    # ------------------
    query_2 = None
    if cube_name == "Results Cube":
        select_2_cols = [f"{x_sql} as \"{x_axis}\""]
        if pivot_col != "None":
            select_2_cols.append(f"{cfg['dimensions'][pivot_col]} as \"{pivot_col}\"")
        select_2_cols.append("SUM(CASE WHEN f.classified_position <= 3 THEN 1 ELSE 0 END) as \"Podiums\"")
        select_2_cols.append("AVG(f.classified_position) as \"Avg_Finish_Position\"")
        
        query_2 = f"SELECT {', '.join(select_2_cols)} FROM {cfg['fact']} {cfg['joins']}"
        
        if slice_dim != "None" and slice_val.strip() != "":
            # `safe_vals_str` and `slice_sql` are already computed in the main query block
            query_2 += f" WHERE {slice_sql} IN ({safe_vals_str})"
        
        query_2 += f" GROUP BY {', '.join(group_cols)}"
        query_2 += f" ORDER BY \"Podiums\" DESC, \"Avg_Finish_Position\" ASC LIMIT 100"

    try:
        start = time.time()
        df = con.sql(query).df()
        df2 = con.sql(query_2).df() if query_2 else None
        duration = time.time() - start
        
        color = pivot_col if pivot_col != "None" else None
        
        title = f"{agg_func} of {y_axis} grouped by {x_axis}"
        if pivot_col != "None":
            title += f" structured by {pivot_col}"
            
        if chart_type == "Bar":
            fig1 = px.bar(df, x=x_axis, y=y_axis, color=color, barmode='stack', title=title)
        elif chart_type == "Line":
            fig1 = px.line(df, x=x_axis, y=y_axis, color=color, title=title)
            fig1.update_traces(line=dict(width=4))
        elif chart_type == "Scatter":
            fig1 = px.scatter(df, x=x_axis, y=y_axis, color=color, title=title)
            fig1.update_traces(marker=dict(size=10))
        else:
            fig1 = px.pie(df, names=x_axis, values=y_axis, title=title)
            
        fig2 = None
        if query_2:
            fig2 = px.bar(df2, x=x_axis, y="Podiums", color=color, barmode='stack', title=f"Podiums & Average Positions by {x_axis}", 
                          hover_data=["Avg_Finish_Position"])
            # Update trace with secondary axis for Avg Finish Position or simply keep it simple via hover info like above

        out_sql = f"✅ Rendered in {duration:.2f}s.\n\n**Generated DuckDB Query:**\n```sql\n{query}\n```"
        if query_2:
            out_sql += f"\n\n**Generated Secondary Plot Query:**\n```sql\n{query_2}\n```"

        return fig1, fig2 if fig2 else gr.Plot(visible=False), out_sql
        
    except Exception as e:
        return None, None, f"❌ Error querying dataframe: {str(e)}"

def run_arm(min_supp, min_conf, target_metric, slice_dim, slice_val):
    global con
    if con is None:
        return None, "❌ Database not connected!"
    
    try:
        query = f"""
            SELECT 
                l.is_personal_best,
                l.compound,
                l.tyre_life,
                w.rainfall,
                w.track_temp,
                r.classified_position,
                r.points_scored,
                r.position_gain
            FROM fact_laps l
            JOIN dim_sessions s ON l.session_key = s.session_key
            LEFT JOIN (
                SELECT session_key, MAX(TRY_CAST(rainfall AS FLOAT)) as rainfall, AVG(TRY_CAST(track_temp AS FLOAT)) as track_temp 
                FROM dim_weather 
                GROUP BY session_key
            ) w ON l.session_key = w.session_key
            LEFT JOIN fact_race_results r ON l.session_key = r.session_key AND l.driver_key = r.driver_key
            WHERE l.compound IS NOT NULL AND l.compound != 'UNKNOWN'
        """
        
        if slice_dim != "None" and slice_val.strip() != "":
            # Mapping slice dimensions for ARM/Clustering based on Results Cube context (common dimensions)
            dim_map = {
                "Driver": "d.name",
                "Team": "t.name",
                "Session": "s.session_name",
                "Event": "s.event_name",
                "Circuit": "c.name",
                "Year": "EXTRACT(YEAR FROM s.date::TIMESTAMP)"
            }
            # Need to join others if slicing by Team/Driver/Circuit
            if slice_dim == "Driver":
                query = query.replace("FROM fact_laps l", "FROM fact_laps l JOIN dim_drivers d ON l.driver_key = d.driver_key")
            elif slice_dim == "Team":
                query = query.replace("FROM fact_laps l", "FROM fact_laps l JOIN dim_teams t ON l.team_key = t.team_key")
            elif slice_dim == "Circuit":
                query = query.replace("FROM fact_laps l", "FROM fact_laps l JOIN dim_circuits c ON s.circuit_id = c.circuit_key") # schema check needed

            slice_sql = dim_map[slice_dim]
            safe_vals = [v.strip().replace("'", "''") for v in slice_val.split(",")]
            safe_vals_str = ", ".join(f"'{v}'" for v in safe_vals)
            query += f" AND {slice_sql} IN ({safe_vals_str})"

        df = con.sql(query).df()
        
        # Build transactions based on lap features
        transactions = []
        for _, row in df.iterrows():
            t = []
            t.append(f"Compound: {row['compound']}")
            
            if pd.notna(row['tyre_life']):
                tl = int(row['tyre_life'])
                if tl < 5: t.append("Tyre_Life: < 5")
                elif tl <= 15: t.append("Tyre_Life: 5-15")
                else: t.append("Tyre_Life: > 15")
                
            if pd.notna(row['is_personal_best']):
                t.append(f"Personal Best: {bool(row['is_personal_best'])}")
                
            if pd.notna(row['rainfall']):
                t.append("Rainfall: Yes" if float(row['rainfall']) > 0 else "Rainfall: No")
                
            if pd.notna(row['track_temp']):
                tt = float(row['track_temp'])
                if tt < 25: t.append("Track_Temp: < 25C")
                elif tt <= 40: t.append("Track_Temp: 25-40C")
                else: t.append("Track_Temp: > 40C")
                
            if pd.notna(row['classified_position']):
                t.append("Podium Finish: True" if int(row['classified_position']) <= 3 else "Podium Finish: False")
                
            if pd.notna(row['points_scored']):
                t.append("Points Scored: True" if float(row['points_scored']) > 0 else "Points Scored: False")
                
            if pd.notna(row['position_gain']):
                t.append("Position Gained: True" if int(row['position_gain']) > 0 else "Position Gained: False")
                
            transactions.append(t)
        
        te = TransactionEncoder()
        te_ary = te.fit(transactions).transform(transactions)
        df_trans = pd.DataFrame(te_ary, columns=te.columns_)
        
        frequent_itemsets = fpgrowth(df_trans, min_support=min_supp, use_colnames=True)
        if len(frequent_itemsets) == 0:
             return pd.DataFrame(), "No frequent itemsets found. Lower minimum support."
             
        rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_conf)
        
        if len(rules) == 0:
            return pd.DataFrame(), "Found frequent itemsets, but no rules met the confidence threshold."
            
        rules["antecedents"] = rules["antecedents"].apply(lambda x: ', '.join(list(x)))
        rules["consequents"] = rules["consequents"].apply(lambda x: ', '.join(list(x)))
        
        if target_metric != "None":
            rules = rules[rules["consequents"].str.contains(target_metric, regex=False)]
            
        if len(rules) == 0:
            return pd.DataFrame(), f"No rules found with consequent containing '{target_metric}'."
        
        rules = rules.sort_values('confidence', ascending=False)
        return rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']].round(3), "✅ ARM executed successfully."
        
    except Exception as e:
        return pd.DataFrame(), f"❌ ML Execution Error: {str(e)}"

def run_clustering(n_clusters, segment_type, slice_dim, slice_val):
    global con
    if con is None:
        return None, "❌ Database not connected!"
        
    try:
        dim_map = {
            "Driver": "d.name",
            "Team": "t.name",
            "Session": "s.session_name",
            "Event": "s.event_name",
            "Circuit": "c.name",
            "Year": "EXTRACT(YEAR FROM s.date::TIMESTAMP)"
        }
        
        where_clause = "WHERE f.speed < 200 AND f.throttle_pct < 50" if segment_type == "Corners" else "WHERE f.speed > 250 AND f.throttle_pct > 90"
        
        if slice_dim != "None" and slice_val.strip() != "":
            slice_sql = dim_map[slice_dim]
            safe_vals = [v.strip().replace("'", "''") for v in slice_val.split(",")]
            safe_vals_str = ", ".join(f"'{v}'" for v in safe_vals)
            where_clause += f" AND {slice_sql} IN ({safe_vals_str})"

        if segment_type == "Corners":
            # Corners: brake timing (proxy: avg brake bool), downshift timing (avg gear), speed
            query = f"""
                SELECT 
                    d.name as driver_name,
                    AVG(CAST(f.brake_applied AS INTEGER)) as avg_brake,
                    AVG(f.gear) as avg_gear,
                    AVG(f.speed) as avg_speed
                FROM fact_telemetry f
                JOIN fact_laps l ON f.lap_key = l.lap_key
                JOIN dim_drivers d ON l.driver_key = d.driver_key
                JOIN dim_sessions s ON l.session_key = s.session_key
                {where_clause}
                GROUP BY d.name
            """
            axes = ['avg_brake', 'avg_gear', 'avg_speed']
            title = "Driver Style: Corners (Brake, Gear, Speed)"
        else:
            # Straights: gear, speed, throttle
            query = f"""
                SELECT 
                    d.name as driver_name,
                    AVG(f.gear) as avg_gear,
                    AVG(f.speed) as avg_speed,
                    AVG(f.throttle_pct) as avg_throttle
                FROM fact_telemetry f
                JOIN fact_laps l ON f.lap_key = l.lap_key
                JOIN dim_drivers d ON l.driver_key = d.driver_key
                JOIN dim_sessions s ON l.session_key = s.session_key
                {where_clause}
                GROUP BY d.name
            """
            axes = ['avg_gear', 'avg_speed', 'avg_throttle']
            title = "Driver Style: Straights (Gear, Speed, Throttle)"

        df = con.sql(query).df().dropna()
        if len(df) < n_clusters:
            return gr.Plot(visible=False), f"Not enough drivers with {segment_type} telemetry data to form clusters."
            
        X = df[axes]
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init='auto')
        df['Cluster'] = kmeans.fit_predict(X).astype(str)
        
        fig = px.scatter_3d(df, x=axes[0], y=axes[1], z=axes[2],
                            color='Cluster', hover_name='driver_name', title=title)
                            
        return fig, f"✅ KMeans clustered {segment_type} telemetry successfully."
        
    except Exception as e:
        return gr.Plot(visible=False), f"❌ ML Execution Error: {str(e)}"

def predict_pit_strategy(lap_number, tyre_life, track_temp, rainfall, compound, finishing_pos, current_pos):
    global con
    if con is None:
        return "❌ Database not connected!"
    
    try:
        clf_path = os.path.join(MODELS_DIR, "rfc_classifier.pkl")
        reg_path = os.path.join(MODELS_DIR, "rfr_regressor.pkl")
        # 1. Fetch historical data with 'Strategy Quality' weighting
        # We calculate grid_gain to penalize 'missed opportunities' (starting high, finishing low)
        query = f"""
            SELECT 
                l.lap_number,
                l.tyre_life,
                l.current_position,
                w.track_temp,
                w.rainfall,
                l.compound,
                r.classified_position,
                r.position_gain,
                (TRY_CAST(raw_r.grid_position AS INTEGER) - TRY_CAST(raw_r.classified_position AS INTEGER)) as total_race_gain,
                CASE WHEN l.pit_in_time IS NOT NULL THEN 1 ELSE 0 END as pitted
            FROM fact_laps l
            JOIN dim_weather w ON l.session_key = w.session_key
            JOIN fact_race_results r ON l.session_key = r.session_key AND l.driver_key = r.driver_key
            JOIN results raw_r ON r.result_key = raw_r.id
            WHERE l.tyre_life IS NOT NULL 
              AND w.track_temp IS NOT NULL 
              AND r.classified_position IS NOT NULL 
              AND l.current_position IS NOT NULL
              AND raw_r.status = 'Finished'
        """
        df = con.sql(query).df()
        
        if df.empty or df['pitted'].sum() < 5:
            return "⚠️ Not enough historical high-quality data to evaluate strategy."

        # 2. Preprocess & Weighting
        # 'Missed Opportunities' are drivers who lost a lot of places relative to their start (negative total_race_gain).
        # We weight the models to favor samples where total_race_gain was high.
        df['is_soft'] = (df['compound'] == 'SOFT').astype(int)
        df['is_medium'] = (df['compound'] == 'MEDIUM').astype(int)
        df['is_hard'] = (df['compound'] == 'HARD').astype(int)
        
        # New Feature: position_delta (Difference between target classified position and current position)
        df['position_delta'] = df['classified_position'] - df['current_position']
        
        # Calculate weights: higher weight for positive gain, lower for missed opportunities
        # We normalize weights to be between 0.1 and 2.0
        gain = df['total_race_gain'].clip(lower=-10, upper=10)
        sample_weights = (gain - gain.min()) / (gain.max() - gain.min() + 1e-6)
        sample_weights = 0.1 + (sample_weights * 1.9)

        features = ['lap_number', 'tyre_life', 'track_temp', 'rainfall', 'is_soft', 'is_medium', 'is_hard', 'position_delta']
        X = df[features].values.astype(float)
        y_clf = df['pitted'].values
        y_reg = df['position_gain'].values
        
        # 3. Handle Classifier (Should I Pit?)
        if os.path.exists(clf_path):
            clf = joblib.load(clf_path)
            loaded_clf = True
        else:
            clf = RandomForestClassifier(n_estimators=100, random_state=42)
            clf.fit(X, y_clf, sample_weight=sample_weights) # Apply weights here
            joblib.dump(clf, clf_path)
            loaded_clf = False

        # 4. Handle Regressor (Expected Position Gain)
        if os.path.exists(reg_path):
            reg = joblib.load(reg_path)
            loaded_reg = True
        else:
            reg = RandomForestRegressor(n_estimators=100, random_state=42)
            reg.fit(X, y_reg, sample_weight=sample_weights) # Apply weights here
            joblib.dump(reg, reg_path)
            loaded_reg = False
        
        # 5. Predict
        input_data = pd.DataFrame([{
            'lap_number': lap_number,
            'tyre_life': tyre_life,
            'track_temp': track_temp,
            'rainfall': 1 if rainfall else 0,
            'is_soft': 1 if compound == 'SOFT' else 0,
            'is_medium': 1 if compound == 'MEDIUM' else 0,
            'is_hard': 1 if compound == 'HARD' else 0,
            'position_delta': finishing_pos - current_pos
        }])
        X_test = input_data[features].values.astype(float)
        
        prob = clf.predict_proba(X_test)[0][1]
        expected_gain = reg.predict(X_test)[0]
        
        status_msg = f" (Opportunity-Weighted Models)" if (loaded_clf and loaded_reg) else f" (Opportunity-Weighted Models)"
        
        if prob > 0.7:
            advice = "🚨 **HIGH CONFIDENCE**: Box this lap! Model reflects aggressive, successful historical strategy."
        elif prob > 0.3:
            advice = "⚠️ **MODERATE**: Model suggests monitoring tyres. High-opportunity strategies are mixed here."
        else:
            advice = "✅ **STAY OUT**: Model indicates staying out matches historically optimal gains."
            
        gain_msg = f"\n\n📈 **Opportunity-Weighted Gain**: {expected_gain:+.2f} spots (Optimized for success)"
            
        return f"{advice} (Score: {prob:.2f}){gain_msg}{status_msg}"

        
    except Exception as e:
        import traceback
        return f"❌ Prediction Error: {str(e)}\n{traceback.format_exc()}"

with gr.Blocks(title="F1 OLAP Dashboard Analytics") as demo:
    gr.Markdown("# Formula 1 OLAP Engine 🏎️")
    
    with gr.Row():
        use_local_checkbox = gr.Checkbox(label="Use Local DB?", value=False)
        local_db_input = gr.Textbox(label="Local DB Path (e.g. 2025.db)", value="2025.db", interactive=True)
        connect_btn = gr.Button("🔌 Connect to DuckDB")
        status_text = gr.Markdown("Status: Not Connected")
        
    connect_btn.click(fn=connect_db, inputs=[use_local_checkbox, local_db_input], outputs=[status_text])

    with gr.Tabs():
        with gr.Tab("Cube Operations"):
            with gr.Row():
                with gr.Column(scale=1):
                    cube_radio = gr.Radio(choices=list(CUBE_CONFIG.keys()), label="1. Which Data Cube?", value="Results Cube", interactive=True)
                    x_axis_dd = gr.Dropdown(label="Group By (X-axis)", choices=["Driver", "Team", "Session", "Event", "Circuit", "Year"], value="Driver", interactive=True)
                    pivot_dd = gr.Dropdown(label="Pivot By / Color Categories", choices=["None", "Driver", "Team", "Session", "Event", "Circuit", "Year"], value="None", interactive=True)
                    y_axis_dd = gr.Dropdown(label="Measure (Y-axis)", choices=["points_scored", "position_gain", "laps_completed"], value="points_scored", interactive=True)
                    agg_func_dd = gr.Radio(label="Aggregation Math", choices=["SUM", "AVG", "MAX"], value="SUM", interactive=True)
                    slice_dim_dd = gr.Dropdown(label="Slice Dimension", choices=["None", "Driver", "Team", "Session", "Event", "Circuit", "Year"], value="None", interactive=True)
                    slice_val_txt = gr.Textbox(label="Slice Value(s)", placeholder="e.g. Lando Norris, Max Verstappen", interactive=True)
                    chart_type_dd = gr.Dropdown(label="Rendering Type", choices=["Bar", "Line", "Scatter", "Pie"], value="Bar", interactive=True)
                    query_btn = gr.Button("Generate Insight Plot", variant="primary")
                    
                with gr.Column(scale=2):
                    with gr.Row():
                        plot_output_1 = gr.Plot()
                        plot_output_2 = gr.Plot()
                    query_output = gr.Markdown("### Built SQL Engine output will appear here")

            cube_radio.change(fn=update_cube_options, inputs=[cube_radio], outputs=[x_axis_dd, y_axis_dd, pivot_dd, slice_dim_dd, chart_type_dd, slice_val_txt])
            query_btn.click(fn=evaluate_olap_query, inputs=[cube_radio, x_axis_dd, y_axis_dd, pivot_dd, slice_dim_dd, slice_val_txt, agg_func_dd, chart_type_dd], outputs=[plot_output_1, plot_output_2, query_output])

        with gr.Tab("Pattern Mining"):
            with gr.Tabs():
                with gr.Tab("Association Rule Mining (Market Basket)"):
                    gr.Markdown("Find what factors lead to a personal best lap or other outcomes.")
                    with gr.Row():
                        with gr.Column(scale=1):
                            target_metric_dd = gr.Dropdown(label="Target Consequent", choices=["None", "Personal Best: True", "Podium Finish: True", "Points Scored: True", "Position Gained: True"], value="Personal Best: True")
                            min_supp_slider = gr.Slider(minimum=0.01, maximum=1.0, value=0.01, step=0.01, label="Minimum Support")
                            min_conf_slider = gr.Slider(minimum=0.01, maximum=1.0, value=0.1, step=0.01, label="Minimum Confidence")
                            slice_dim_arm = gr.Dropdown(label="Slice Dimension", choices=["None", "Driver", "Team", "Session", "Event", "Circuit", "Year"], value="None")
                            slice_val_arm = gr.Textbox(label="Slice Value(s)", placeholder="e.g. 2024")
                            run_arm_btn = gr.Button("Run FP-Growth", variant="primary")
                        with gr.Column(scale=2):
                            arm_output_msg = gr.Markdown("Status: Ready")
                            arm_output_df = gr.Dataframe(interactive=False)
                    run_arm_btn.click(fn=run_arm, inputs=[min_supp_slider, min_conf_slider, target_metric_dd, slice_dim_arm, slice_val_arm], outputs=[arm_output_df, arm_output_msg])

                with gr.Tab("Telemetry Clustering (Driver Styles)"):
                    gr.Markdown("Group drivers implicitly based on their telemetry metrics.")
                    with gr.Row():
                        with gr.Column(scale=1):
                            segment_type_dd = gr.Dropdown(label="Race Segment", choices=["Corners", "Straights"], value="Corners")
                            k_clusters_slider = gr.Slider(minimum=2, maximum=10, value=3, step=1, label="Number of Clusters (K)")
                            slice_dim_clust = gr.Dropdown(label="Slice Dimension", choices=["None", "Driver", "Team", "Session", "Event", "Circuit", "Year"], value="None")
                            slice_val_clust = gr.Textbox(label="Slice Value(s)", placeholder="e.g. 2024")
                            run_cluster_btn = gr.Button("Run K-Means", variant="primary")
                        with gr.Column(scale=2):
                            cluster_output_msg = gr.Markdown("Status: Ready")
                            cluster_output_plot = gr.Plot()
                    run_cluster_btn.click(fn=run_clustering, inputs=[k_clusters_slider, segment_type_dd, slice_dim_clust, slice_val_clust], outputs=[cluster_output_plot, cluster_output_msg])

        with gr.Tab("ML Prediction"):
            gr.Markdown("### Strategy Predictor: Should I Pit Next Lap?")
            gr.Markdown("Enter your current race state to see if historical winning patterns suggest a pit stop.")
            with gr.Row():
                with gr.Column():
                    pred_lap = gr.Number(label="Current Lap", value=20)
                    pred_tyre_life = gr.Number(label="Tyre Age (Laps)", value=15)
                    pred_temp = gr.Slider(minimum=10, maximum=60, value=35, label="Track Temp (°C)")
                    pred_rain = gr.Checkbox(label="Is it Raining?", value=False)
                    pred_compound = gr.Dropdown(choices=["SOFT", "MEDIUM", "HARD"], value="SOFT", label="Current Compound")
                    pred_pos_curr = gr.Slider(minimum=1, maximum=20, value=10, step=1, label="Current Race Position")
                    pred_pos = gr.Slider(minimum=1, maximum=20, value=10, step=1, label="Target Finishing Position")
                    predict_btn = gr.Button("Evaluate Strategy", variant="primary")
                with gr.Column():
                    prediction_output = gr.Markdown("### Analysis Result will appear here")
            
            predict_btn.click(
                fn=predict_pit_strategy, 
                inputs=[pred_lap, pred_tyre_life, pred_temp, pred_rain, pred_compound, pred_pos, pred_pos_curr], 
                outputs=[prediction_output]
            )

if __name__ == "__main__":
    demo.launch()
