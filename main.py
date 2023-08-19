import streamlit as st
from pymongo import MongoClient
from googleClientYoutubeData import dataExtraction
from toSql import sqlStoring, channelData, playlistData, videoData

# Initializing MongoDB Connection
client = MongoClient('mongodb://localhost:27017/')
db = client['youtube_data_db']
collection = db['youtube_data_collection']

# Title
st.title('Youtube Data Extraction')

# Inputs
channel_id = st.text_input('Enter Youtube Channel ID:')
API_KEY = st.text_input('Enter API KEY:')


# Functions
def check_input_exists(channel):
    # Check if the input value exists in the collection
    result = collection.find_one({'Channel_Info.Channel_Id': channel})
    return result is None


def fetch_channel_details(channel_name):
    query = {"Channel_Info.Channel_Name": channel_name}
    channel_info = collection.find_one(query)
    return channel_info.get("Channel_Info") if channel_info else []


# Button
if st.button('Extract & Store'):
    if check_input_exists(channel_id):
        dataFromYoutube = dataExtraction(API_KEY, channel_id)
        collection.insert_one(dataFromYoutube)
        st.write('Data have been collected and stored into MongoDB')
    else:
        st.write('The Channel_ID is already present in the database')

channel_name = [doc["Channel_Info"]["Channel_Name"] for doc in
                collection.find({}, {"Channel_Info.Channel_Name": 1, "_id": 0}) if
                "Channel_Info" in doc and "Channel_Name" in doc["Channel_Info"]]

# Radio Button
selected_channel = st.radio("Select a YouTube Channel", channel_name)
if st.button('Initiate Storing in SQL'):
    channel_details = fetch_channel_details(selected_channel)
    sqlStoring(channel_details)
    st.write('Data added to the table')
    st.table(channelData())
    st.table(playlistData())
    st.table(videoData())

