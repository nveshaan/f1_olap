import gradio as gr
import plotly.express as px
import pandas as pd
from db_manager import get_connection
from cubes import setup_cubes
import time

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

if __name__ == "__main__":
    demo.launch()
