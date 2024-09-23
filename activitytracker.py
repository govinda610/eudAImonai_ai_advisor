import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime

# Initialize SQLite database
conn = sqlite3.connect('activity_tracker.db', check_same_thread=False)
c = conn.cursor()

# Create table if not exists
c.execute('''CREATE TABLE IF NOT EXISTS activities
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
              category TEXT,
              timestamp DATETIME,
              location TEXT)''')

# Create table for categories if not exists
c.execute('''CREATE TABLE IF NOT EXISTS categories
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT UNIQUEP)''')

conn.commit()

def add_category(category):
    try:
        c.execute("INSERT INTO categories (name) VALUES (?)", (category,))
        conn.commit()
        st.success(f"Category '{category}' added successfully!")
    except sqlite3.IntegrityError:
        st.error(f"Category '{category}' already exists!")

def delete_category(category):
    c.execute("DELETE FROM categories WHERE name=?", (category,))
    conn.commit()
    st.success(f"Category '{category}' deleted successfully!")

def get_categories():
    c.execute("SELECT name FROM categories")
    return [row[0] for row in c.fetchall()]

def add_activity(category, location):
    timestamp = datetime.now()
    c.execute("INSERT INTO activities (category, timestamp, location) VALUES (?, ?, ?)",
              (category, timestamp, location))
    conn.commit()
    st.success(f"Activity recorded: {category} at {location}")

def get_activities():
    c.execute("SELECT * FROM activities")
    return pd.DataFrame(c.fetchall(), columns=['id', 'category', 'timestamp', 'location'])

# Streamlit app
st.title("Activity Tracker App")

# Sidebar for adding/deleting categories
st.sidebar.header("Manage Categories")
new_category = st.sidebar.text_input("Add new category")
if st.sidebar.button("Add Category"):
    add_category(new_category)

categories = get_categories()
category_to_delete = st.sidebar.selectbox("Select category to delete", categories)
if st.sidebar.button("Delete Category"):
    delete_category(category_to_delete)

# Main app - Record activity
st.header("Record Activity")
selected_category = st.selectbox("Select activity", categories)
location = st.text_input("Enter location")
if st.button("Record Activity"):
    add_activity(selected_category, location)

# View activities
st.header("View Activities")

# Get all activities
df = get_activities()

# Convert timestamp to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Filter options
st.subheader("Filter Options")
start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")
selected_locations = st.multiselect("Select Locations", df['location'].unique())

# Apply filters
mask = (df['timestamp'].dt.date >= start_date) & (df['timestamp'].dt.date <= end_date)
if selected_locations:
    mask &= df['location'].isin(selected_locations)
filtered_df = df[mask]

# Display total count
st.subheader("Total Count of Activities")
activity_counts = filtered_df['category'].value_counts()
st.write(activity_counts)

# Data Visualization
st.subheader("Activity Distribution")
st.bar_chart(activity_counts)

# View individual instances
st.header("View Individual Instances")
st.dataframe(filtered_df)

# Close the database connection when the app is done
conn.close()