import googleapiclient
from googleapiclient.discovery import build

def dataExtraction(api_key, channel_id):
    # Initialize YouTube API client
    youtube = build('youtube', 'v3', developerKey=api_key)

    # Fetch channel data from YouTube
    channel_response = youtube.channels().list(part="snippet,statistics", id=channel_id).execute()
    channel_data = channel_response['items'][0]

    output = {
        "Channel_Info": {
            "Channel_Name": channel_data['snippet']['title'],
            "Channel_Id": channel_id,
            "Subscription_Count": channel_data['statistics']['subscriberCount'],
            "Channel_Views": channel_data['statistics']['viewCount'],
            "Channel_Description": channel_data['snippet']['description'],
            "Playlists": []
        },
        "Video_Info": []
    }

    # Fetch playlist IDs and titles for the channel
    playlist_response = youtube.playlists().list(part="snippet", channelId=channel_id).execute()
    playlist_items = playlist_response.get('items', [])

    for item in playlist_items:
        playlist_info = {
            "Playlist_Id": item['id'],
            "Playlist_Title": item['snippet']['title']
        }
        output["Channel_Info"]["Playlists"].append(playlist_info)

    for playlist_info in output["Channel_Info"]["Playlists"]:
        playlist_id = playlist_info["Playlist_Id"]
        playlist_items_response = youtube.playlistItems().list(part="contentDetails", playlistId=playlist_id,
                                                               maxResults=50).execute()
        video_ids = [item['contentDetails']['videoId'] for item in playlist_items_response['items']]

        for video_id in video_ids:
            video_response = youtube.videos().list(part="snippet,statistics,contentDetails", id=video_id).execute()
            if 'items' in video_response and len(video_response['items']) > 0:
                video_data = video_response['items'][0]
                try:
                    # Fetch comments for the video
                    comments_response = youtube.commentThreads().list(part="snippet", videoId=video_id,
                                                                      maxResults=50).execute()
                    comments = []
                    for item in comments_response['items']:
                        comment = item['snippet']['topLevelComment']['snippet']
                        comment_info = {
                            "Comment_Id": item['id'],
                            "Comment_Text": comment['textDisplay'],
                            "Comment_Author": comment['authorDisplayName'],
                            "Comment_PublishedAt": comment['publishedAt']
                        }
                        comments.append(comment_info)

                    like_count = video_data['statistics'].get('likeCount', 0)
                    video_info = {
                        "Video_Id": video_data['id'],
                        "Video_Name": video_data['snippet']['title'],
                        "Video_Description": video_data['snippet']['description'],
                        "Tags": video_data['snippet']['tags'] if 'tags' in video_data['snippet'] else [],
                        "PublishedAt": video_data['snippet']['publishedAt'],
                        "View_Count": video_data['statistics']['viewCount'],
                        "Like_Count": like_count,
                        "Dislike_Count": video_data['statistics'].get('dislikeCount', 0),
                        "Favorite_Count": video_data['statistics']['favoriteCount'],
                        "Comment_Count": video_data['statistics']['commentCount'],
                        "Duration": video_data['contentDetails']['duration'],
                        "Thumbnail": video_data['snippet']['thumbnails']['default']['url'],
                        "Caption_Status": video_data['contentDetails']['caption'],
                        "Comments": comments
                    }
                    output["Video_Info"].append(video_info)

                except googleapiclient.errors.HttpError as e:
                    if 'commentsDisabled' in str(e):
                        print(f"Comments are disabled for Video ID: {video_id}")
                    else:
                        print(f"Error fetching comments for Video ID: {video_id}")

    return output
