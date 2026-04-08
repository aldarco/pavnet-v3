import sqlite3
import json
import time
import matplotlib
matplotlib.use('Qt5Agg')

import matplotlib.pyplot as plt
import matplotlib.animation as animation
from datetime import datetime
from collections import defaultdict

# --- CONFIGURATION ---
DB_PATH = "localdb_XXX.db"   # CHANGE THIS
TABLE_NAME = "localbackup"   # CHANGE THIS IF DIFFERENT
POLL_INTERVAL = 5000         # milliseconds (1 second)
MAX_POINTS = 1000

# --- Helper functions ---
def get_all_data(conn):
    """Return list of (datetime, amplitudes_dict) sorted by timestamp."""
    cur = conn.cursor()
    cur.execute(f"SELECT timestamp, data FROM {TABLE_NAME} ORDER BY timestamp ASC")
    rows = cur.fetchall()
    data = []
    for ts_str, data_json in rows:
        try:
            record = json.loads(data_json)
            ts = record.get('timestamp', ts_str)
            dt = datetime.fromisoformat(ts)
            amps = record.get('amplitudes', {})
            data.append((dt, amps))
        except Exception as e:
            print(f"Error parsing row: {e}")
    return data

def to_seconds(dt, start_time):
    return (dt - start_time).total_seconds()

# --- Setup figure ---
fig, ax = plt.subplots()
ax.set_xlabel("Time (seconds from start)")
ax.set_ylabel("Amplitude")
ax.set_title("Real‑time amplitudes from DB")

colors = plt.cm.tab10.colors

# Get reference start time from first record (if any)
conn = sqlite3.connect(DB_PATH)
first_rows = get_all_data(conn)[:1]
if first_rows:
    start_dt = first_rows[0][0]
else:
    start_dt = None
conn.close()

def update_plot(frame):
    """Called periodically by FuncAnimation."""
    if start_dt is None:
        # No data yet, just clear and return
        ax.clear()
        ax.set_title("Waiting for data...")
        return

    conn = sqlite3.connect(DB_PATH)
    all_data = get_all_data(conn)
    conn.close()

    if not all_data:
        return

    # Build fresh data series (or you could update incrementally)
    new_plot_data = defaultdict(lambda: ([], []))
    for dt, amps in all_data:
        t_sec = to_seconds(dt, start_dt)
        for ch, val in amps.items():
            times, values = new_plot_data[ch]
            times.append(t_sec)
            values.append(val)
            if len(times) > MAX_POINTS:
                times.pop(0)
                values.pop(0)

    # Redraw
    ax.clear()
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Amplitude")
    ax.set_title("Real‑time amplitudes from DB")
    for i, (ch, (times, values)) in enumerate(new_plot_data.items()):
        if times:
            ax.plot(times, values, marker='o', linestyle='-',
                    color=colors[i % len(colors)], label=ch)
    ax.legend()
    fig.tight_layout()

# Create animation
ani = animation.FuncAnimation(fig, update_plot, interval=POLL_INTERVAL, cache_frame_data=False)

print("Viewer started. Close the plot window to exit.")
plt.show()
