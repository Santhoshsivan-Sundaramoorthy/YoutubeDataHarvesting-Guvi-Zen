import streamlit as st
from pymongo import MongoClient
from googleClientYoutubeData import dataExtraction

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


# Button
if st.button('Extract & Store'):
    if check_input_exists(channel_id):
        dataFromYoutube = dataExtraction(API_KEY, channel_id)
        collection.insert_one(dataFromYoutube)
        st.write('Data have been collected and stored into MongoDB')

    else:
        st.write('The Channel_ID is already present in the database')
