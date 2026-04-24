import gradio as gr
import plotly.express as px
import pandas as pd
from db_manager import get_connection
from cubes import setup_cubes
import time
from sklearn.cluster import KMeans
from mlxtend.frequent_patterns import fpgrowth, association_rules
from mlxtend.preprocessing import TransactionEncoder

con = None

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
            "Circuit": "c.name"
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
            "Lap Number": "f.lap_number"
        },
        "measures": ["tyre_life", "lap_time_sec", "s1_time", "s2_time", "s3_time"] # ms metrics
    },
    "Telemetry Cube": {
        "fact": "fact_telemetry f",
        "joins": "JOIN fact_laps l ON f.lap_key = l.lap_key JOIN dim_drivers d ON l.driver_key = d.driver_key JOIN dim_sessions s ON l.session_key = s.session_key",
        "dimensions": {
            "Driver": "d.name",
            "Session": "s.session_name",
            "DRS Status": "f.drs_status"
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

def run_arm(min_supp, min_conf, target_metric):
    global con
    if con is None:
        return None, "❌ Database not connected!"
    
    try:
        query = """
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
            LEFT JOIN (
                SELECT session_key, MAX(TRY_CAST(rainfall AS FLOAT)) as rainfall, AVG(TRY_CAST(track_temp AS FLOAT)) as track_temp 
                FROM dim_weather 
                GROUP BY session_key
            ) w ON l.session_key = w.session_key
            LEFT JOIN fact_race_results r ON l.session_key = r.session_key AND l.driver_key = r.driver_key
            WHERE l.compound IS NOT NULL AND l.compound != 'UNKNOWN'
        """
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

def run_clustering(n_clusters):
    global con
    if con is None:
        return None, "❌ Database not connected!"
        
    try:
        query = """
            SELECT 
                d.name as driver_name,
                AVG(f.speed) as avg_speed,
                AVG(f.throttle_pct) as avg_throttle,
                AVG(f.rpm) as avg_rpm
            FROM fact_telemetry f
            JOIN fact_laps l ON f.lap_key = l.lap_key
            JOIN dim_drivers d ON l.driver_key = d.driver_key
            WHERE f.speed IS NOT NULL AND f.throttle_pct IS NOT NULL
            GROUP BY d.name
        """
        df = con.sql(query).df().dropna()
        if len(df) < n_clusters:
            return gr.Plot(visible=False), "Not enough drivers with telemetry data to form requested clusters."
            
        X = df[['avg_speed', 'avg_throttle', 'avg_rpm']]
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init='auto')
        df['Cluster'] = kmeans.fit_predict(X).astype(str)
        
        fig = px.scatter_3d(df, x='avg_speed', y='avg_throttle', z='avg_rpm',
                            color='Cluster', hover_name='driver_name', title="Driver Styles Clustering (Telemetry)")
                            
        return fig, "✅ KMeans clustered driver telemetry successfully."
        
    except Exception as e:
        return gr.Plot(visible=False), f"❌ ML Execution Error: {str(e)}"

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
                    x_axis_dd = gr.Dropdown(label="Group By (X-axis)", choices=["Driver", "Team", "Session", "Event", "Circuit"], value="Driver", interactive=True)
                    pivot_dd = gr.Dropdown(label="Pivot By / Color Categories", choices=["None", "Driver", "Team", "Session", "Event", "Circuit"], value="None", interactive=True)
                    y_axis_dd = gr.Dropdown(label="Measure (Y-axis)", choices=["points_scored", "position_gain", "laps_completed"], value="points_scored", interactive=True)
                    agg_func_dd = gr.Radio(label="Aggregation Math", choices=["SUM", "AVG"], value="SUM", interactive=True)
                    slice_dim_dd = gr.Dropdown(label="Slice Dimension", choices=["None", "Driver", "Team", "Session", "Event", "Circuit"], value="None", interactive=True)
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
            gr.Markdown("### Discover Hidden Patterns via ML Algorithms")
            with gr.Tabs():
                with gr.Tab("Association Rule Mining (Market Basket)"):
                    gr.Markdown("Find what factors lead to a personal best lap or other outcomes.")
                    with gr.Row():
                        with gr.Column(scale=1):
                            target_metric_dd = gr.Dropdown(label="Target Consequent", choices=["None", "Personal Best: True", "Personal Best: False", "Rainfall: Yes", "Compound: SOFT"], value="Personal Best: True")
                            min_supp_slider = gr.Slider(minimum=0.01, maximum=1.0, value=0.01, step=0.01, label="Minimum Support")
                            min_conf_slider = gr.Slider(minimum=0.01, maximum=1.0, value=0.1, step=0.01, label="Minimum Confidence")
                            run_arm_btn = gr.Button("Run FP-Growth", variant="primary")
                        with gr.Column(scale=2):
                            arm_output_msg = gr.Markdown("Status: Ready")
                            arm_output_df = gr.Dataframe(interactive=False)
                    run_arm_btn.click(fn=run_arm, inputs=[min_supp_slider, min_conf_slider, target_metric_dd], outputs=[arm_output_df, arm_output_msg])

                with gr.Tab("Telemetry Clustering (Driver Styles)"):
                    gr.Markdown("Group drivers implicitly based on their telemetry metrics (Speed, Throttle, RPM).")
                    with gr.Row():
                        with gr.Column(scale=1):
                            k_clusters_slider = gr.Slider(minimum=2, maximum=10, value=3, step=1, label="Number of Clusters (K)")
                            run_cluster_btn = gr.Button("Run K-Means", variant="primary")
                        with gr.Column(scale=2):
                            cluster_output_msg = gr.Markdown("Status: Ready")
                            cluster_output_plot = gr.Plot()
                    run_cluster_btn.click(fn=run_clustering, inputs=[k_clusters_slider], outputs=[cluster_output_plot, cluster_output_msg])

if __name__ == "__main__":
    demo.launch()
