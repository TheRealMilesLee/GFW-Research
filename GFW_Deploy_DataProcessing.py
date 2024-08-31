import pandas as pd
import plotly.express as px
import re
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import time

def clean_data(input_file):
  print("Cleaning and processing data...")
  with open(input_file, 'r') as f:
    lines = f.readlines()

  data = []
  for line in lines:
    domain = line.split(':')[0].strip()
    if 'No GFW detected' in line:
      status = 'No GFW'
      ip = ''
      location = ''
    elif 'Traceroute timed out' in line:
      status = 'Timed Out'
      ip = ''
      location = ''
    elif 'Possible GFW detected at' in line:
      status = 'GFW Detected'
      match = re.search(r'at ([\d\.]+) \((.*?)\)', line)
      if match:
        ip = match.group(1)
        location = match.group(2)
      else:
        ip = ''
        location = ''
    else:
      status = 'Unknown'
      ip = ''
      location = ''

    data.append({
      'Domain': domain,
      'Status': status,
      'IP': ip,
      'Location': location
    })

  df = pd.DataFrame(data)
  df[['Country', 'Region', 'City']] = df['Location'].str.split(', ', expand=True)

  return df

def get_coordinates(location, geolocator, retries=3, delay=1):
  for attempt in range(retries):
    try:
      location = geolocator.geocode(location)
      if location:
        return location.latitude, location.longitude
      return None, None
    except (GeocoderTimedOut, GeocoderUnavailable):
      if attempt < retries - 1:
        time.sleep(delay)
      else:
        print(f"Geocoding failed for {location} after {retries} attempts")
        return None, None

def add_coordinates(df):
  print("Adding coordinates...")
  geolocator = Nominatim(user_agent="gfw_analyzer")
  df['Latitude'] = None
  df['Longitude'] = None

  for index, row in df.iterrows():
    if row['Status'] == 'GFW Detected':
      location = f"{row['City']}, {row['Region']}, {row['Country']}"
      lat, lon = get_coordinates(location, geolocator)
      df.at[index, 'Latitude'] = lat
      df.at[index, 'Longitude'] = lon

  return df

def visualize_data(df, output_file):
  print("Creating visualization...")
  df_map = df[df['Status'] == 'GFW Detected'].dropna(subset=['Latitude', 'Longitude'])

  fig = px.scatter_geo(df_map,
             lat='Latitude',
             lon='Longitude',
             color='Status',
             hover_name='Domain',
             hover_data=['IP', 'Country', 'Region', 'City'],
             title='GFW Detection Results')

  fig.update_layout(
    geo=dict(
      showland=True,
      showcountries=True,
      showocean=True,
      countrywidth=0.5,
      landcolor='rgb(243, 243, 243)',
      oceancolor='rgb(220, 240, 255)',
    )
  )

  fig.write_html(output_file)
  print(f"Interactive map saved to {output_file}")

def print_summary(df):
  print("\nSummary Statistics:")
  print(df['Status'].value_counts())
  print("\nTop 5 Countries where GFW was detected:")
  print(df[df['Status'] == 'GFW Detected']['Country'].value_counts().head())

def main(input_file, output_file):
  df = clean_data(input_file)
  df = add_coordinates(df)
  visualize_data(df, output_file)
  print_summary(df)

  # Save processed data to CSV
  csv_output = output_file.replace('.html', '.csv')
  df.to_csv(csv_output, index=False)
  print(f"Processed data saved to {csv_output}")

if __name__ == "__main__":
  main("gfw_results.txt", "gfw_detection_map.html")
