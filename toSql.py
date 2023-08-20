import re
import sqlite3

def tableCreation():
    # Connect to the SQLite database
    conn = sqlite3.connect('youtube_data1.db')
    cursor = conn.cursor()
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS channel (
                channel_id VARCHAR(255) PRIMARY KEY,
                channel_name VARCHAR(255),
                channel_views INT,
                channel_description TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS playlist (
                playlist_id VARCHAR(255) PRIMARY KEY,
                playlist_title VARCHAR(255),
                channel_id VARCHAR(255) REFERENCES channel(channel_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS video (
                video_id VARCHAR(255) PRIMARY KEY,
                video_name VARCHAR(255),
                video_description TEXT,
                tags TEXT,
                published_at DATETIME,
                view_count INT,
                like_count INT,
                dislike_count INT,
                favorite_count INT,
                comment_count INT,
                duration INT,
                thumbnail VARCHAR(255),
                caption_status BOOLEAN,
                channel_id VARCHAR(255) REFERENCES channel(channel_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS comment (
                comment_id VARCHAR(255) PRIMARY KEY,
                comment_text TEXT,
                comment_author VARCHAR(255),
                comment_published_at DATETIME,
                video_id VARCHAR(255) REFERENCES video(video_id)
            )
        ''')

        print('Table Done')
    except sqlite3.Error as e:
        print("Error:", e)
    conn.close()

def sqlStoring(channel_data_list):
    # Connect to the SQLite database
    conn = sqlite3.connect('youtube_data1.db')
    cursor = conn.cursor()
    if 'Channel_Name' in channel_data_list:
        cursor.execute('''
                    INSERT OR REPLACE INTO channel (channel_id, channel_name, channel_views, channel_description)
                    VALUES (?, ?, ?, ?)
                ''', (
            channel_data_list['Channel_Id'],
            channel_data_list['Channel_Name'],
            int(channel_data_list['Subscription_Count']),
            channel_data_list['Channel_Description']
        ))
        print('CHANNEL added')
    else:
        print("'Channel_Name' key not found in the document")

    playlists = channel_data_list.get('Playlists', [])
    for playlist in playlists:
        cursor.execute('''
                INSERT OR REPLACE INTO playlist (channel_id, playlist_id, playlist_title)
                VALUES (?, ?, ?)
            ''', (
            channel_data_list['Channel_Id'],
            playlist['Playlist_Id'],
            playlist['Playlist_Title']
        ))
        print('PLAYLIST added')

    videos = channel_data_list.get('Video_Info', [])
    for video in videos:
        duration = video['Duration']
        seconds = iso8601_duration_to_seconds(duration)

        cursor.execute('''
            INSERT OR REPLACE INTO video (
                video_id, video_name, video_description, tags, published_at,
                view_count, like_count, dislike_count, favorite_count,
                comment_count, duration, thumbnail, caption_status, channel_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            video['Video_Id'],
            video['Video_Name'],
            video['Video_Description'],
            ', '.join(video['Tags']),
            video['PublishedAt'],
            int(video['View_Count']),
            int(video['Like_Count']),
            int(video['Dislike_Count']),
            int(video['Favorite_Count']),
            int(video['Comment_Count']),
            seconds,
            video['Thumbnail'],
            video['Caption_Status'],
            channel_data_list['Channel_Id']
        ))
        print('VIDEO added')

        for comment in video['Comments']:
            cursor.execute('''
                INSERT OR IGNORE INTO comment (
                    comment_id, video_id, comment_text, comment_author, comment_published_at
                )
                VALUES (?, ?, ?, ?, ?)
            ''', (
                comment['Comment_Id'],
                video['Video_Id'],
                comment['Comment_Text'],
                comment['Comment_Author'],
                comment['Comment_PublishedAt']
            ))
            print('COMMENT added')

    conn.commit()
    conn.close()


def channelData():
    try:
        conn = sqlite3.connect('youtube_data1.db')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM channel')
        channel_data = cursor.fetchall()
        conn.close()
        return channel_data
    except Exception as e:
        return None



def playlistData(channel_id):
    conn = sqlite3.connect('youtube_data1.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM playlist WHERE channel_id = ?', (channel_id,))
    playlist_data = cursor.fetchall()
    conn.close()
    return playlist_data


def videoData(channel_id):
    conn = sqlite3.connect('youtube_data1.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM video WHERE channel_id = ?', (channel_id,))
    video_data = cursor.fetchall()
    conn.close()
    return video_data


def commentData(video_Id):
    conn = sqlite3.connect('youtube_data1.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM comment WHERE Video_Id = ?', (video_Id,))
    comment_data = cursor.fetchall()
    conn.close()
    return comment_data

def iso8601_duration_to_seconds(duration):
    duration_pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = duration_pattern.match(duration)


    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)

    total_seconds =  (hours * 3600) + (minutes * 60) + seconds
    return total_seconds