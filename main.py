import sqlite3

import pandas as pd
import streamlit as st
from pymongo import MongoClient
from googleClientYoutubeData import dataExtraction
from toSql import sqlStoring, channelData, playlistData, videoData, commentData, tableCreation

tableCreation()
with st.container():
    image_path = "youtube.png"
    image = st.image(image_path, width=50)
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


def playlistTableDesign(data):
    # Convert data to DataFrame
    columns = ['Playlist ID', 'Playlist Name', 'Channel ID']
    original_df = pd.DataFrame(data, columns=columns)

    # Create a new DataFrame with rearranged columns
    new_columns_order = ['Playlist Name', 'Playlist ID', 'Channel ID']
    new_df = original_df[new_columns_order]

    modified_df = new_df.style.set_properties(**{'text-align': 'left'})
    return modified_df


def videoTableDesign(data):
    # Convert data to DataFrame
    columns = ['Video ID', 'Video Title', 'Description', 'Tags', 'Published Date', 'View Count', 'Like Count',
               'Dislike Count', 'Favorite Count', 'Comment Count', 'Duration', 'Thumbnail', 'Caption Status',
               'Channel ID']
    original_df = pd.DataFrame(data, columns=columns)

    # Create a new DataFrame with rearranged columns
    new_columns_order = ['Video Title', 'Description', 'Video ID', 'Tags', 'Published Date', 'View Count', 'Like Count',
                         'Dislike Count', 'Favorite Count', 'Comment Count', 'Duration', 'Thumbnail', 'Caption Status',
                         'Channel ID']
    new_df = original_df[new_columns_order]

    return new_df


def commentTableDesign(data):
    # Convert data to DataFrame
    columns = ['Comment ID', 'Comment', 'Commented By', 'Time', 'Video ID']
    original_df = pd.DataFrame(data, columns=columns)

    # Create a new DataFrame with rearranged columns
    new_columns_order = ['Comment', 'Commented By', 'Time', 'Comment ID', 'Video ID']
    new_df = original_df[new_columns_order]

    modified_df = new_df.style.set_properties(**{'text-align': 'left'})
    return modified_df


with col1:
    # Inputs
    channel_id = st.text_input('Enter Youtube Channel ID:', key='channel_id_input')
    API_KEY = st.text_input('Enter API KEY:', key='api_key_input')

    if channel_id == '' or API_KEY == '':
        st.warning('Please enter both the Channel ID and API Key.')
    elif st.button('Extract & Store'):
        if check_input_exists(channel_id):
            dataFromYoutube = dataExtraction(API_KEY, channel_id)
            collection.insert_one(dataFromYoutube)
            st.success('Data have been collected and stored into MongoDB')
        else:
            st.warning('The Channel ID is already present in the database')

channel_name = [doc["Channel_Info"]["Channel_Name"] for doc in
                collection.find({}, {"Channel_Info.Channel_Name": 1, "_id": 0}) if
                "Channel_Info" in doc and "Channel_Name" in doc["Channel_Info"]]


def questionQuery(query1):
    connection = sqlite3.connect('youtube_data1.db')
    cursor = connection.cursor()
    cursor.execute(query1)
    results = cursor.fetchall()
    connection.close()
    return results


with col2:
    # Radio Button
    selected_channel = st.radio("Select a YouTube Channel", channel_name)
    channel_id1 = fetch_channel_id(selected_channel)
    if st.button('Initiate Storing in SQL and view the details'):
        channel_details = fetch_channel_details(selected_channel)
        sqlStoring(channel_details)
        st.write('Data added to the table')

with col3:
    data = channelData()
    channelChangedTableDesign = channelTableDesign(data)
    st.write('All Channel Data')
    st.write(channelChangedTableDesign)

playlist_Data = playlistData(channel_id1)
playlistChangedTableDesign = playlistTableDesign(playlist_Data)
st.write(f'Playlist Data of {selected_channel}')
st.write(playlistChangedTableDesign)

video_data = videoData(channel_id1)
newdf = videoTableDesign(video_data)
st.write(f'Video Data of {selected_channel}')
st.write(newdf)

selected_video = st.selectbox('Select a video title', newdf['Video Title'])
with st.container():
    if selected_video:
        selected_video_id = newdf[newdf['Video Title'] == selected_video]['Video ID'].values[0]
        comment_Data = commentData(selected_video_id)
        commentChangedTable = commentTableDesign(comment_Data)
        st.write(f'Comment Data of {selected_video}')
        st.write(commentChangedTable)

questions = ['What are the names of all the videos and their corresponding channels?',
             'Which channels have the most number of videos, and how many videos do they have?',
             'What are the top 10 most viewed videos and their respective channels?',
             'How many comments were made on each video, and what are their corresponding video names?',
             'Which videos have the highest number of likes, and what are their corresponding channel names?',
             'What is the total number of likes and dislikes for each video, and what are their corresponding video '
             'names?',
             'What is the total number of views for each channel, and what are their corresponding channel names?',
             'What are the names of all the channels that have published videos in the year 2022?',
             'What is the average duration of all videos in each channel, and what are their corresponding channel '
             'names?',
             'Which videos have the highest number of comments, and what are their corresponding channel names?']

selectedQuestion = st.selectbox('Select the Question:', questions)
if selectedQuestion:
    if selectedQuestion == questions[0]:
        query = """
            SELECT v.video_name, c.channel_name
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
        """
        results = questionQuery(query)
        column = ['Video Title', 'Channel Name']
        df = pd.DataFrame(results, columns=column)
        st.write(df)

    if selectedQuestion == questions[1]:
        c1, c2 = st.columns(2)
        query = """
            SELECT c.channel_name, COUNT(v.video_id) AS video_count
            FROM channel c
            JOIN video v ON c.channel_id = v.channel_id
            GROUP BY c.channel_name
            ORDER BY video_count DESC
        """
        results = questionQuery(query)
        column = ['Channel Name', 'Video Count']
        df = pd.DataFrame(results, columns=column)
        with c1:
            st.write(df)
        with c2:
            st.bar_chart(df.set_index('Channel Name'))

    if selectedQuestion == questions[2]:
        c1, c2 = st.columns(2)
        query = """
            SELECT v.video_name, c.channel_name, v.view_count
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
            ORDER BY v.view_count DESC
            LIMIT 10
        """
        results = questionQuery(query)
        column = ['Video Name', 'Channel Name', 'View Count']
        df = pd.DataFrame(results, columns=column)
        with c1:
            st.write(df)
        with c2:
            # Group data by channel and calculate total view count
            grouped_df = df.groupby('Channel Name')['View Count'].sum().reset_index()

            # Display bar plot using Streamlit's native bar_chart function
            st.bar_chart(grouped_df.set_index('Channel Name'))

    if selectedQuestion == questions[3]:
        c1, c2 = st.columns(2)
        query = """
            SELECT v.video_name, COUNT(c.comment_id) AS comment_count
            FROM video v
            LEFT JOIN comment c ON v.video_id = c.video_id
            GROUP BY v.video_name
        """
        results = questionQuery(query)
        column = ['Video Name', 'Comment Count']
        df = pd.DataFrame(results, columns=column)
        with c1:
            st.write(df)
        with c2:
            st.bar_chart(df.set_index('Video Name'))
    if selectedQuestion == questions[4]:
        c1, c2 = st.columns(2)
        query = """
            SELECT v.video_name, c.channel_name, v.like_count
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
            ORDER BY v.like_count DESC
            LIMIT 10
        """
        results = questionQuery(query)
        column = ['Video Name', 'Channel Name', 'Like Count']
        df = pd.DataFrame(results, columns=column)
        with c1:
            st.write(df)
        with c2:
            # Group data by channel and calculate total view count
            grouped_df = df.groupby('Video Name')['Like Count'].sum().reset_index()
            st.bar_chart(grouped_df.set_index('Video Name'))

    if selectedQuestion == questions[5]:
        c1, c2 = st.columns(2)
        query = """
            SELECT v.video_name, SUM(v.like_count) AS total_likes, SUM(v.dislike_count) AS total_dislikes
            FROM video v
            GROUP BY v.video_name
        """
        results = questionQuery(query)
        column = ['Video Name', 'Like', 'Dislike']
        df = pd.DataFrame(results, columns=column)
        with c1:
            st.write(df)
        with c2:
            grouped_df = df.groupby('Video Name')['Like'].sum().reset_index()
            st.bar_chart(grouped_df.set_index('Video Name'))
    if selectedQuestion == questions[6]:
        c1, c2 = st.columns(2)
        query = """
            SELECT c.channel_name, SUM(v.view_count) AS total_views
            FROM channel c
            JOIN video v ON c.channel_id = v.channel_id
            GROUP BY c.channel_name
        """
        results = questionQuery(query)
        column = ['Channel Name', 'Total View']
        df = pd.DataFrame(results, columns=column)
        with c1:
            st.write(df)
        with c2:
            st.bar_chart(df.set_index('Channel Name'))
    if selectedQuestion == questions[7]:
        query = """
            SELECT DISTINCT c.channel_name
            FROM channel c
            JOIN video v ON c.channel_id = v.channel_id
            WHERE strftime('%Y', v.published_at) = '2022'
        """

        results = questionQuery(query)
        column = ['Channel Name']
        df = pd.DataFrame(results, columns=column)
        st.write(df)

    if selectedQuestion == questions[8]:
        c1, c2 = st.columns(2)
        query = """
    SELECT c.channel_name, AVG(duration) AS avg_duration
    FROM channel c
    JOIN video v ON c.channel_id = v.channel_id
    GROUP BY c.channel_name
"""

        results = questionQuery(query)
        column = ['Channel Name', 'Average Duration(Seconds)']
        df = pd.DataFrame(results, columns=column)
        with c1:
            st.write(df)
        with c2:
            st.bar_chart(df.set_index('Channel Name'))

    if selectedQuestion == questions[9]:
        c1, c2 = st.columns(2)
        query = """
            SELECT v.video_name, c.channel_name, v.comment_count
            FROM video v
            JOIN channel c ON v.channel_id = c.channel_id
            ORDER BY v.comment_count DESC
            LIMIT 10
        """
        results = questionQuery(query)
        column = ['Video Name', 'Channel Name', 'Comment Count']
        df = pd.DataFrame(results, columns=column)
        with c1:
            st.write(df)
        with c2:
            st.bar_chart(df.set_index('Video Name')['Comment Count'])
