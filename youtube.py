from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

def Api_connect():
    Api_Id = "AIzaSyDpuyzxlxFP4RYDfnwA22wCvsn7dy66SZc"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_Id)
    return youtube

# Call the function to get the youtube object
youtube = Api_connect()

request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id="UCOKNHJ1y3iiHjvI-FZLiT5g"
)

response1 = request.execute()

# Now you can make API requests
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )

    response = request.execute()

    for i in response["items"]:
        data = dict(
                        Channel_Name = i["snippet"]["title"],
                        Channel_Id = i["id"],
                        Subscription_Count= i["statistics"]["subscriberCount"],
                        Views = i["statistics"]["viewCount"],
                        Total_Videos = i["statistics"]["videoCount"],
                        Channel_Description = i["snippet"]["description"],
                        Playlist_Id = i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    ).execute()
    
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids

# Example usage
#channel_id = 'UCOKNHJ1y3iiHjvI-FZLiT5g'  # Replace with your desired channel ID
#videos = get_video_ids(channel_id)
#print(videos)


def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()
        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

#get comment information
def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:
                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()

                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                                Comment_Information.append(comment_information)
        except:
                pass

        return Comment_Information
    
    
def get_playlist_details(channel_id):
    next_page_token = None
    All_data = []

    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(
                Playlist_Id=item['id'],
                Title=item['snippet']['title'],
                Channel_Id=item['snippet']['channelId'],
                Channel_Name=item['snippet']['channelTitle'],
                PublishedAt=item['snippet']['publishedAt'],
                Video_Count=item['contentDetails']['itemCount']
            )
            All_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break

    return All_data

# Example usage
#channel_id = 'UCOKNHJ1y3iiHjvI-FZLiT5g'  # Replace with your desired channel ID
#playlist_details = get_playlist_details(channel_id)
#print(playlist_details)


# Replace this connection string with your actual MongoDB connection string
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Select or create a database
db = client["youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    
    coll1=db["data_base"]
    coll1.insert_one({"channel_information":ch_details, "playlist_information":pl_details,
                      "video_information":vi_details, "comment_information":com_details})
    
    return "upload completed successfully"

def channels_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="H&@R@n29y@",
        database="youtube_data",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''
            CREATE TABLE IF NOT EXISTS channels(
                Channel_Name VARCHAR(100),
                Channel_Id VARCHAR(80) PRIMARY KEY,
                Subscribers BIGINT,
                Views BIGINT,
                Total_Videos INT,
                Channel_Description TEXT,
                Playlist_Id VARCHAR(50)
            )
        '''
        cursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        print(f"Error creating channels table: {e}")

    # Fetch data from MongoDB
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["data_base"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])

    # Insert data into PostgreSQL with ON CONFLICT clause
    for index, row in pd.DataFrame(ch_list).iterrows():
        insert_query = '''
            INSERT INTO channels(
                Channel_Name,
                Channel_Id,
                Subscribers,
                Views,
                Total_Videos,
                Channel_Description,
                Playlist_Id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Channel_Id) DO NOTHING
        '''
        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscription_Count'],
            row['Views'],
            row['Total_Videos'],
            row['Channel_Description'],
            row['Playlist_Id']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")

    # Close the connections
    #cursor.close()
    #mydb.close()
    #client.close()


def playlists_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="H&@R@n29y@",
        database="youtube_data",
        port="5432"
    )
    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                                                        Title varchar(80),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        VideoCount int
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()
    
    pl_list = []
    db = client["youtube_data"]
    coll1 = db["data_base"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)
    

    for index,row in df1.iterrows():
            insert_query = '''insert into playlists(PlaylistId,
                                                    Title,
                                                    Channel_Id,
                                                    Channel_Name,
                                                    PublishedAt,
                                                    VideoCount)
                                                    
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
            values =(
                    row['Playlist_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    row['PublishedAt'],
                    row['Video_Count'])

            
            cursor.execute(insert_query,values)
            mydb.commit()     
                


def videos_table():
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="H&@R@n29y@",
                database= "youtube_data",
                port = "5432"
                )
    cursor = mydb.cursor()
    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists videos(
                    Channel_Name varchar(150),
                    Channel_Id varchar(100),
                    Video_Id varchar(50) primary key,
                    Title varchar(150),
                    Tags text,
                    Thumbnail varchar(225),
                    Description text,
                    Published_Date timestamp,
                    Duration interval,
                    Views bigint,
                    Likes bigint,
                    Comments int,
                    Favorite_Count int,
                    Definition varchar(10),
                    Caption_Status varchar(50)
                    )'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db = client["youtube_data"]
    coll1 = db["data_base"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
            insert_query = '''
                        INSERT INTO videos (Channel_Name,
                            Channel_Id,
                            Video_Id,
                            Title,
                            Tags,
                            Thumbnail,
                            Description,
                            Published_Date,
                            Duration,
                            Views,
                            Likes,
                            Comments,
                            Favorite_Count,
                            Definition,
                            Caption_Status
                            )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
            values = (
                        row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status'])

        
            cursor.execute(insert_query,values)
            mydb.commit()
            

def comments_table():
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="H&@R@n29y@",
                database= "youtube_data",
                port = "5432"
                )
    cursor = mydb.cursor()
    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()
    create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                    Video_Id varchar(80),
                    Comment_Text text,
                    Comment_Author varchar(150),
                    Comment_Published timestamp)'''
    cursor.execute(create_query)
    mydb.commit()
    
    com_list = []
    db = client["youtube_data"]
    coll1 = db["data_base"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
            for i in range(len(com_data["comment_information"])):
                com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)

    for index, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_Id,
                                        Video_Id ,
                                        Comment_Text,
                                        Comment_Author,
                                        Comment_Published)
                                        VALUES (%s, %s, %s, %s, %s)
                                    '''
            values = (
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
            )
            
            cursor.execute(insert_query,values)
            mydb.commit()

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"

def show_channels_table():
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["data_base"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    
    return channels_table

def show_playlists_table():
    pl_list = []
    db = client["youtube_data"]
    coll1 = db["data_base"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    playlists_table = st.dataframe(pl_list)
    
    return playlists_table

def show_videos_table():
    vi_list = []
    db = client["youtube_data"]
    coll1 = db["data_base"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    
    return videos_table

def show_comments_table():
    com_list = []
    db = client["youtube_data"]
    coll1 = db["data_base"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    
    return comments_table

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Using SQL, MongoDB, and Streamlit")
    st.header(":red[SKILLS TAKE AWAY]")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("Streamlit")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")

channel_id = st.text_input("Enter the Channel ID")

if st.button("Collect and Store data"):
    ch_ids = []
    db = client["youtube_data"]
    coll1 = db["data_base"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
            
    if channel_id in ch_ids:
        st.success("Channel details of the given channel id already exists")
    else:
        insert = channel_details(channel_id)
        st.success(insert)
            
if st.button("Migrate to SQL Database"):
    Table = tables()
    st.success(Table)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[CHANNELS]",":orange[PLAYLISTS]",":red[VIDEOS]",":blue[COMMENTS]"))
if show_table == ":green[CHANNELS]":
    show_channels_table()
elif show_table == ":orange[PLAYLISTS]":
    show_playlists_table()
elif show_table ==":red[VIDEOS]":
    show_videos_table()
elif show_table == ":blue[COMMENTS]":
    show_comments_table()
    
#SQL connection
mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="H&@R@n29y@",
            database= "youtube_data",
            port = "5432"
            )
cursor = mydb.cursor()

question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos and their respective channels',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. Likes of all videos',
     '7. Views of each channel',
     '8. Videos published in the year 2022',
     '9. Average duration of all videos in each channel',
     '10. Videos with highest number of comments'))

if question == "1. All the videos and the Channel Name":
    query1 = '''select title as videos, Channel_Name as ChannelName from videos;'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1, columns=["video title","Channel name"])
    st.write(df)

elif question == "2. Channels with most number of videos":
    query2 = '''select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df1=pd.DataFrame(t2, columns=["channel name","No Of Videos"])
    st.write(df1)
    
elif question == "3. 10 most viewed videos and their respective channels":
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos
                    where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df2=pd.DataFrame(t3, columns=["views","channel Name","videotitle"])
    st.write(df2)
    df2
    
elif question == "4. Comments in each video":
    query4 = '''select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df3=pd.DataFrame(t4, columns=["No Of Comments", "Video Title"])
    st.write(df3)
    
elif question == "5. Videos with highest likes":
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df4=pd.DataFrame(t5, columns=["video Title","channel Name","like count"])
    st.write(df4)
    
elif question == "6. likes of all videos":
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df5=pd.DataFrame(t6, columns=["like count","video title"])
    st.write(df5)
    
elif question == "7. views of each channel":
    query7 = '''select Channel_Name as ChannelName, Views as Channelviews from channels;'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df6=pd.DataFrame(t7, columns=["channel name","total views"])
    st.write(df6)
    
elif question == "8. videos published in the year 2022":
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df7=pd.DataFrame(t8, columns=["Name", "Video Publised On", "ChannelName"])
    st.write(df7)
    
elif question == "9. average duration of all videos in each channel":
    query9 = '''SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df8 = pd.DataFrame(t9, columns=["ChannelName", "average_duration"])
    st.write(df8)

    T9 = []
    for index, row in df8.iterrows():
        channel_title = row['ChannelName']
        average_duration = row['average_duration']
        average_duration_str = str(average_duration)
        T9.append(dict(ChannelTitle=channel_title, avgDuration=average_duration_str))
        df9 =pd.DataFrame(T9)
        st.write(df9)
        
elif question == "10. videos with highest number of comments":
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos
                    where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["VideoTitle", "ChannelName", "Comments"])
    st.write(df10)
    df10


    
    


