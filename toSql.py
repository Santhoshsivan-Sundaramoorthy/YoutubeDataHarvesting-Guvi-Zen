import sqlite3


def sqlStoring(channel_data_list):
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
                duration VARCHAR(50),
                thumbnail VARCHAR(255),
                caption_status BOOLEAN
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

    conn.commit()
    conn.close()


def channelData():
    conn = sqlite3.connect('youtube_data1.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM channel')
    channel_data = cursor.fetchall()
    conn.close()
    return channel_data


