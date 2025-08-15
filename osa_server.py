import sys
import os
import plotly.express as px
import plotly.graph_objects as go
from tcdona3.osa import OSA
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
from datetime import datetime
import io
from flask import send_file
import pandas as pd
import dash_bootstrap_components as dbc

osa = OSA()


def get_osa_reading():
    osa.osa_sweep()
    osa.osa_sweep()
    data = osa.osa_get_data_screen_dashboard()
    y = [float(i) for i in data]
    x = list(range(len(y)))
    return x, y


def get_osa_config():
    config = {}
    config["Identity"] = osa.identify()
    config["Resolution"] = osa.get_resolution()
    config["Attenuation"] = osa.get_attn_status()
    config["Wavelength Start"] = osa.get_wavelength_start()
    config["Wavelength Centre"] = osa.get_wavelength_centre()
    config["Wavelength Span"] = osa.get_wavelength_span()
    config["Wavelength Stop"] = osa.get_wavelength_stop()
    config["Sampling Points"] = osa.get_sampling_points()
    return config


def plot_figure(x, y):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=y, mode="lines", name="OSA Measurement"))
    fig.update_layout(
        title="OSA Measurement", xaxis_title="X-axis", yaxis_title="Y-axis"
    )
    return fig


x, y = get_osa_reading()
config = get_osa_config()
fig = plot_figure(x, y)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the layout
app.layout = dbc.Container(
    [
        dbc.NavbarSimple(
            brand="Optical Spectrum Analyser Dashboard",
            brand_href="#",
            color="primary",
            dark=True,
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Card(
                            [
                                dbc.CardBody(
                                    [
                                        html.H4("Controls", className="card-title"),
                                        html.Button(
                                            "Refresh",
                                            id="refresh-button",
                                            n_clicks=0,
                                            className="btn btn-primary mb-2",
                                        ),
                                        html.Button(
                                            "Download CSV",
                                            id="download-button",
                                            className="btn btn-secondary mb-2",
                                        ),
                                        dcc.Interval(
                                            id="interval",
                                            interval=1000,
                                            n_intervals=0,
                                            disabled=True,
                                        ),  # Interval component for the cooldown
                                        html.Div(
                                            id="timestamp",
                                            children="",
                                            className="mt-2",
                                        ),
                                        dcc.Store(
                                            id="store-data",
                                            data={"x": x, "y": y, "config": config},
                                        ),
                                    ]
                                )
                            ],
                            className="mb-4",
                        ),
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H4(
                                        "OSA Configuration", className="card-title"
                                    ),
                                    dcc.Markdown(
                                        id="additional-data",
                                        children="",
                                        className="mt-2",
                                    ),
                                ]
                            ),
                            className="mb-4",
                        ),
                    ],
                    width=3,
                ),
                dbc.Col(
                    [
                        dbc.Card(
                            dbc.CardBody([dcc.Graph(id="example-graph", figure=fig)]),
                            className="mb-4",
                        ),
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H4(
                                        "Update Configuration", className="card-title"
                                    ),
                                    dbc.Input(
                                        id="wavelength-start-input",
                                        type="text",
                                        placeholder="Wavelength Start",
                                        className="mb-2",
                                    ),
                                    dbc.Button(
                                        "Update Configuration",
                                        id="update-config-button",
                                        color="primary",
                                        className="mb-2",
                                    ),
                                    html.Div(
                                        id="config-status",
                                        children="",
                                        className="mt-2",
                                    ),
                                ]
                            ),
                            className="mb-4",
                        ),
                    ],
                    width=9,
                ),
            ]
        ),
        dcc.Download(id="download"),
    ],
    fluid=True,
)

# Combined callback to handle both updates and configuration changes
@app.callback(
    [
        Output("example-graph", "figure"),
        Output("timestamp", "children"),
        Output("additional-data", "children"),
        Output("store-data", "data"),
        Output("config-status", "children"),
    ],
    [Input("refresh-button", "n_clicks"), Input("update-config-button", "n_clicks")],
    [State("wavelength-start-input", "value"), State("store-data", "data")],
)
def update_and_configure(
    refresh_n_clicks, config_n_clicks, wavelength_start, stored_data
):
    # Determine which button was clicked
    ctx = dash.callback_context

    # Initialize variables
    fig = dash.no_update
    timestamp_text = dash.no_update
    additional_data_text = dash.no_update
    config_status = dash.no_update

    # If refresh button was clicked
    if ctx.triggered and ctx.triggered[0]["prop_id"] == "refresh-button.n_clicks":
        x, y = get_osa_reading()
        fig = plot_figure(x, y)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timestamp_text = f"Last refreshed at {current_time}. The refresh function is locked for 30 seconds."
        stored_data["x"] = x
        stored_data["y"] = y

    # If update config button was clicked
    if ctx.triggered and ctx.triggered[0]["prop_id"] == "update-config-button.n_clicks":
        try:
            # Update the OSA configuration
            osa.set_wavelength_start(wavelength_start)

            # Get updated configuration
            config = get_osa_config()
            stored_data["config"] = config

            # Generate updated additional data text
            additional_data_text = "  \n".join(
                [f"**{key}:** {value}" for key, value in config.items()]
            )
            config_status = "Configuration updated successfully."
        except Exception as e:
            config_status = f"Error updating configuration: {str(e)}"

    # Always update the additional data text if config has changed
    if fig is not dash.no_update:
        config = get_osa_config()
        additional_data_text = "  \n".join(
            [f"**{key}:** {value}" for key, value in config.items()]
        )

    return fig, timestamp_text, additional_data_text, stored_data, config_status


# Refresh button cooldown callback
@app.callback(
    [
        Output("refresh-button", "disabled"),
        Output("interval", "disabled"),
        Output("interval", "n_intervals"),
    ],
    [Input("refresh-button", "n_clicks"), Input("interval", "n_intervals")],
)
def manage_refresh_button(n_clicks, n_intervals):
    if n_clicks == 0:
        return False, True, 0
    # If button clicked, disable and enable interval
    if dash.callback_context.triggered[0]["prop_id"] == "refresh-button.n_clicks":
        return True, False, 0
    # If 30 seconds passed, start button and disable interval
    if n_intervals >= 30:
        return False, True, 0
    raise dash.exceptions.PreventUpdate


# Download csv callback
@app.callback(
    Output("download", "data"),
    Input("download-button", "n_clicks"),
    State("store-data", "data"),
    prevent_initial_call=True,
)
def download_csv(n_clicks, stored_data):
    if n_clicks == 0:
        raise dash.exceptions.PreventUpdate
    df = pd.DataFrame(stored_data)
    return dcc.send_data_frame(df.to_csv, "current_osa_reading.csv")


# Run the app on localhost:20000. Forward this port to local, and add an alias preferably
if __name__ == "__main__":
    app.run_server(debug=True, port=20000)
