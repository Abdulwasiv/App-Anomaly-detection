import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from timesfm import TimesFM  # Assuming TimesFM is available for anomaly detection

# Load the data (for example, from ClickHouse)
# For demonstration, let's assume data is in a CSV file
data = pd.read_csv('data.csv', usecols=['eventDateTime', 'cpuTotalPercentage'])

# Convert eventDateTime to datetime format
data['eventDateTime'] = pd.to_datetime(data['eventDateTime'])

# Sort by eventDateTime if necessary
data.sort_values(by='eventDateTime', inplace=True)

# Train the TimesFM model on historical data
model = TimesFM(
    
)
model.fit(data['cpuTotalPercentage'])

# Setup real-time plot
plt.ion()  # Interactive mode on
fig, ax = plt.subplots()
line, = ax.plot([], [], 'b-')  # Blue line for data
anomaly_dot, = ax.plot([], [], 'ro')  # Red dots for anomalies

# Function to update plot
def update_plot(x_data, y_data, anomalies):
    line.set_data(x_data, y_data)
    anomaly_dot.set_data(anomalies[0], anomalies[1])
    ax.relim()
    ax.autoscale_view()
    plt.draw()
    plt.pause(0.01)  # Pause to update the plot

# Real-time data processing
# Simulate real-time streaming from ClickHouse or another source
x_data, y_data, anomalies = [], [], [[], []]

for index, row in data.iterrows():
    x_data.append(row['eventDateTime'])
    y_data.append(row['cpuTotalPercentage'])

    # Predict anomaly
    is_anomaly = model.predict(row['cpuTotalPercentage'])
    if is_anomaly:
        anomalies[0].append(row['eventDateTime'])
        anomalies[1].append(row['cpuTotalPercentage'])

    # Update the plot
    update_plot(x_data, y_data, anomalies)

    # Simulate real-time by waiting a second
    time.sleep(1)
