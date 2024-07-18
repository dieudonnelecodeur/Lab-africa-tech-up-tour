import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from decimal import Decimal


st.set_page_config(
    page_title="Filghts Dashboard",
    page_icon="💰",
    layout="wide"
    )
client = MongoClient('mongodb+srv://admin:TXuZcjVneOZfpFfn@cluster0.sz0t89d.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client.kpi_graph

# Convertir les collections en DataFrames
def convert_to_dataframe(collection):
    data = list(collection.find())
    for record in data:
        for key, value in record.items():
            if isinstance(value, Decimal128):
                record[key] = value.to_decimal()
            elif isinstance(value, Decimal):
                record[key] = float(value)
    return pd.DataFrame(data)

total_flights_per_period_col = db.total_flights_per_period
delayed_flights_per_week_col = db.delayed_flights_per_week
average_delay_time_col = db.average_delay_time
top_airports_by_departures_col = db.top_airports_by_departures
flight_distance_passengers_col = db.flight_distance_passengers
avg_fill_rate_per_class_col = db.avg_fill_rate_per_class
average_passengers_per_flight_col = db.average_passengers_per_flight
last_week_revenue_col = db.last_week_revenue
flights_over_time_col = db.flights_over_time
flights_lines_col = db.flights_lines

total_flights_per_period = convert_to_dataframe(total_flights_per_period_col)
delayed_flights_per_week = convert_to_dataframe(delayed_flights_per_week_col)
average_delay_time = convert_to_dataframe(average_delay_time_col)
top_airports_by_departures = convert_to_dataframe(top_airports_by_departures_col)
flight_distance_passengers = convert_to_dataframe(flight_distance_passengers_col)
avg_fill_rate_per_class = convert_to_dataframe(avg_fill_rate_per_class_col)
average_passengers_per_flight = convert_to_dataframe(average_passengers_per_flight_col)
last_week_revenue = convert_to_dataframe(last_week_revenue_col)
flights_over_time = convert_to_dataframe(flights_over_time_col)
flights_lines = convert_to_dataframe(flights_lines_col)

# Convertir les types de colonnes pour flights_over_time
flights_over_time['day'] = pd.to_datetime(flights_over_time['day'])
flights_over_time['num_flights'] = flights_over_time['num_flights'].astype(int)


# Titre du tableau de bord
st.title("Flights Dashboard")

# En-tête des KPIs
# st.header('KPIs')

# Ligne 1 des KPIs
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    num_flights_last_week = int(total_flights_per_period.iloc[0]['total_flights'])
    num_flights_last_2week = int(total_flights_per_period.iloc[1]['total_flights'])
    delta = num_flights_last_week - num_flights_last_2week
    st.metric(label="Nombre total de vols (Semaine dernière)", value=num_flights_last_week, delta=delta)

with col2:
    delayed_flights_last_week = int(delayed_flights_per_week.iloc[0]['delayed_flights'])
    delayed_flights_last_2week = int(delayed_flights_per_week.iloc[1]['delayed_flights'])
    delta = delayed_flights_last_week - delayed_flights_last_2week
    st.metric(label="Nombre de vols en retard", value=delayed_flights_last_week, delta=delta, delta_color="inverse")

with col3:
    average_passengers_per_flight_last_week = round(float(average_passengers_per_flight.iloc[0]['average_passengers']), 2)
    average_passengers_per_flight_last_2week = round(float(average_passengers_per_flight.iloc[1]['average_passengers']), 2)
    delta = round(average_passengers_per_flight_last_week - average_passengers_per_flight_last_2week, 2)
    st.metric(label="Nombre moyen de passagers par vol", value=average_passengers_per_flight_last_week, delta=delta)

with col4:
    average_delay_time_last_week = round(float(average_delay_time.iloc[0]['average_delay_minutes']), 2)
    average_delay_time_last_2week = round(float(average_delay_time.iloc[1]['average_delay_minutes']), 2)
    delta = round(average_delay_time_last_week - average_delay_time_last_2week, 2)
    st.metric(label="Temps moyen de retard des vols (Minutes)", value=average_delay_time_last_week, delta=delta, delta_color="inverse")

with col5:
    last_week_revenue_last_week = round(float(last_week_revenue.iloc[0]['total_revenue']), 2)
    last_week_revenue_last_week_str = f"{round(last_week_revenue_last_week/1000000000, 2)}B $"
    last_week_revenue_last_2week = round(float(last_week_revenue.iloc[1]['total_revenue']), 2)
    delta = round((last_week_revenue_last_week - last_week_revenue_last_2week)/1000000000, 2)
    st.metric(label="Revenus la semaine dernière", value=last_week_revenue_last_week_str, delta=delta)

# Ligne 2 : Évolution du nombre de vols au fil du temps
col1 = st.columns(1)[0]
data = flights_over_time.sort_values(by=["day"], ascending=True)
daily_flights_fig = px.line(x=data["day"], y= data['num_flights'], title="Évolution du nombre de vols journaliers")
daily_flights_fig.update_traces(line=dict(color='#82f4b1'))
daily_flights_fig.update_layout(
    xaxis=dict(
        title='Date',
        titlefont=dict(color='#82f4b1'),
        linecolor='#82f4b1',
        linewidth=1,
        tickfont=dict(color='#82f4b1')
    ),
    yaxis=dict(
        title='Vols',
        titlefont=dict(color='#82f4b1'),
        linecolor='#82f4b1',
        linewidth=1,
        tickfont=dict(color='#82f4b1')
    ),
    showlegend=False
)
st.plotly_chart(daily_flights_fig)

# Ligne 3 : Top aéroports par nombre de départs et Relation entre distance et nombre de passagers
col1, col2 = st.columns(2)

with col1:
    data = top_airports_by_departures.sort_values(by="num_departures", ascending=True)
    data['airport_name'] = data['airport_name'].apply(eval)
    data['name'] = data['airport_name'].apply(lambda x: x['en'])
    bar_chart = px.bar(
        data,
        x='num_departures',
        y='name',
        title="Nombre de départs par aéroport",
        labels={'name': 'Nom', 'num_departures': 'Nombre de départs'}
    )
    bar_chart.update_layout(
        xaxis=dict(
            title="Nombre de départs",
            titlefont=dict(color='#82f4b1'),
            tickfont=dict(color='#82f4b1')
        ),
        yaxis=dict(
            title="Nom de l'aeroport",
            titlefont=dict(color='#82f4b1'),
            tickfont=dict(color='#82f4b1')
        ),
        showlegend=False
    )
    # Mettre à jour les traces pour définir la couleur des barres
    bar_chart.update_traces(marker_color='#82f4b1')
    st.plotly_chart(bar_chart)

with col2:

    # Récupérer les données des vols
    flight_list = list(flights_lines_col.find())

    # Listes pour stocker les coordonnées des lignes et des marqueurs
    lines = []
    markers = []

    for flight in flight_list:
        # Extraire les coordonnées
        departure_coords = tuple(map(float, flight["departure_coordinates"][1:-1].split(",")))
        arrival_coords = tuple(map(float, flight["arrival_coordinates"][1:-1].split(",")))
        departure_city = eval(flight["departure_city"])["en"]
        arrival_city = eval(flight["arrival_city"])["en"]


        # Ajouter les lignes
        lines.append(
            go.Scattergeo(
                lon=[departure_coords[0], arrival_coords[0]],
                lat=[departure_coords[1], arrival_coords[1]],
                mode='lines',
                line=dict(width=1, color='#82f4b1'),
                opacity=0.5,
            )
        )

        # Ajouter des marqueurs pour les aéroports de départ et d'arrivée
        markers.append(
            go.Scattergeo(
                lon=[departure_coords[0]],
                lat=[departure_coords[1]],
                mode='markers',
                marker=dict(size=5, color='blue'),
                name=departure_city
            )
        )
        markers.append(
            go.Scattergeo(
                lon=[arrival_coords[0]],
                lat=[arrival_coords[1]],
                mode='markers',
                marker=dict(size=5, color='red'),
                name=arrival_city
            )
        )

    # Création de la figure
    fig = go.Figure()

    # Ajout des lignes et des marqueurs à la figure
    for line in lines:
        fig.add_trace(line)
    for marker in markers:
        fig.add_trace(marker)

    # Mise en forme de la carte
    fig.update_layout(
        title='Carte des vols',
        showlegend=False,
        geo=dict(
            projection_type='equirectangular',
            showland=True,
            landcolor='#212a3e',  # Couleur du fond de la carte
            countrycolor='#82f4b1',  # Couleur des contours des continents
        ),
        plot_bgcolor='#212a3e',  # Couleur de fond de l'ensemble du graphique
        paper_bgcolor='#212a3e',  # Couleur de fond du papier (espace autour du graphique)
    )

    # Afficher la carte dans Streamlit
    st.plotly_chart(fig, use_container_width=True)

# Fermer la connexion MongoDB
client.close()