import pandas as pd
import streamlit as st
from pymongo import MongoClient
from googleClientYoutubeData import dataExtraction
from toSql import sqlStoring, channelData, playlistData, videoData, commentData

st.subheader('Youtube Data Extraction')
# Initializing MongoDB Connection
client = MongoClient('mongodb://localhost:27017/')
db = client['youtube_data_db']
collection = db['youtube_data_collection']

# Adding Column
col1, col2, col3 = st.columns(3)
# Functions
def check_input_exists(channel):
    # Check if the input value exists in the collection
    result = collection.find_one({'Channel_Info.Channel_Id': channel})
    return result is None


def fetch_channel_details(channel_name):
    query = {"Channel_Info.Channel_Name": channel_name}
    channel_info = collection.find_one(query)
    return channel_info.get("Channel_Info") if channel_info else []


def fetch_channel_id(channel_name):
    query = {"Channel_Info.Channel_Name": channel_name}
    channel_info = collection.find_one(query)

    if channel_info:
        return channel_info["Channel_Info"]["Channel_Id"]
    else:
        return None

def channelTableDesign(data):
    # Convert data to DataFrame
    columns = ['Channel Id', 'Channel Name', 'Like counts', 'Description']
    original_df = pd.DataFrame(data, columns=columns)

    # Create a new DataFrame with rearranged columns
    new_columns_order = ['Channel Name', 'Channel Id', 'Like counts', 'Description']
    new_df = original_df[new_columns_order]

    modified_df = new_df.style.set_properties(**{'text-align': 'left'})
    return modified_df


with col1:
    # Inputs
    channel_id = st.text_input('Enter Youtube Channel ID:')
    API_KEY = st.text_input('Enter API KEY:')
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



with col2:
    # Radio Button
    selected_channel = st.radio("Select a YouTube Channel", channel_name)
    channel_id1 = fetch_channel_id(selected_channel)
    if st.button('Initiate Storing in SQL'):
        channel_details = fetch_channel_details(selected_channel)
        sqlStoring(channel_details)
        st.write('Data added to the table')



with col3:
    data = channelData()
    channelChangedTableDesign = channelTableDesign(data)
    st.write('All Channel Data')
    st.write(channelChangedTableDesign)


st.dataframe(playlistData(channel_id1))
st.dataframe(videoData(channel_id1))

