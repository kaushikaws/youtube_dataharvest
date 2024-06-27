import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
from sqlalchemy import create_engine
import pandas as pd
import mysql.connector

# API key connection
def api_connection():
    Api_Id = "AIzaSyB1omsRXp_RPF8AfTLgGSujDqVItIdhVAk"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_Id)
    return youtube

youtube = api_connection()

# Get channel info
def get_channel_info(Channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=Channel_id)
    response = request.execute()
    for i in response['items']:
        data = dict(
            Channel_Name=i["snippet"]["title"],
            Channel_id=i["id"],
            Subscribers=i['statistics']['subscriberCount'],
            Views=i["statistics"]['viewCount'],
            Total_Videos=i["statistics"]["videoCount"],
            Channel_Description=i["snippet"]["description"],
            Playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"]
        )
    return data

# Get video IDs
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

# Get video info
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id)
        response = request.execute()

        for item in response["items"]:
            tags = item['snippet'].get('tags', [])
            tags_str = ', '.join(tags)  # Convert list to comma-separated string
            duration = item['contentDetails']['duration']
            # Convert ISO 8601 duration format to HH:MM:SS
            duration = duration.replace('PT', '').replace('H', ':').replace('M', ':').replace('S', '')

            data = dict(
                Channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=tags_str,  # Store tags as a string
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description', ''),
                Published_Date=item['snippet']['publishedAt'],
                Duration=duration,  # Store duration as HH:MM:SS
                Views=int(item['statistics'].get('viewCount', 0) or 0),  # Handle None by defaulting to 0
                Likes=int(item['statistics'].get('likeCount', 0) or 0),  # Handle None by defaulting to 0
                Comments=int(item['statistics'].get('commentCount', 0) or 0),  # Handle None by defaulting to 0
                Favorite_Count=int(item['statistics'].get('favoriteCount', 0) or 0),  # Handle None by defaulting to 0
                Definition=item['contentDetails']['definition'],
                Caption_Status=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data



# Get comment info
def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=50)
            response = request.execute()

            for item in response['items']:
                data = dict(
                    Comment_Id=item['snippet']['topLevelComment']['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                Comment_data.append(data)
    except:
        pass
    return Comment_data

# Get playlist info
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

# MongoDB connection
connection_string = "localhost:27017"
client = MongoClient(connection_string)
db = client["youtube"]
coll1 = db["channel_details"]

# Upload channel details to MongoDB
def upload_channel_details(channel_id):
    if coll1.find_one({"channel_information.Channel_id": channel_id}):
        st.warning(f"Channel ID {channel_id} already exists.")
        return
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)
    coll1.insert_one({
        "channel_information": ch_details,
        "playlist_information": pl_details,
        "video_information": vi_details,
        "comment_information": com_details
    })
    return "Channel details uploaded successfully"

# Streamlit cache clear
st.cache_data.clear()
# Streamlit sidebar
st.header(':red[YOUTUBE DATA HARVESTING AND WAREHOUSING]', divider='rainbow')  
# Channel id input
channel_ids = st.text_input("Enter the Channel IDs (comma separated)",help="Please enter the YouTube channel IDs separated by commas.", placeholder="e.g., UC_x5XG1OV2P6uZZ5FSM9Ttw, UCJZv4d5rbIKd4QHMPkcABCw",label_visibility="visible").split(',')
channel_ids = [ch.strip() for ch in channel_ids if ch]
# Channel button 
if st.button("Upload Channel Details to MongoDB"):
    for channel_id in channel_ids:
        if channel_id:
            result = upload_channel_details(channel_id)
            if result:
                st.success(result)
# AWS RDS creds
rds_user = 'admin'
rds_password = 'administrator'
rds_host = 'database.cd6808ymyl01.ap-south-1.rds.amazonaws.com'
rds_port = '3306'
rds_db = 'youtube'
# Create RDS tables
def channels_table():
    connection = mysql.connector.connect(
        host=rds_host,
        user=rds_user,
        password=rds_password,
        database=rds_db,
        port=rds_port
    )
    cursor = connection.cursor()
    try:
        create_db_query = """ create table if not exists channels(Channel_Name varchar(100),
                                                                Channel_Id varchar(80) primary key,
                                                                Subscribers bigint,
                                                                Views bigint,
                                                                Total_Videos int,
                                                                Channel_Description text,
                                                                Playlist_Id varchar(80))
                                                                """
        cursor.execute(create_db_query)
    except:
        st.write("channels table already created")

    ch_list = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)
    engine = create_engine(f'mysql+mysqlconnector://{rds_user}:{rds_password}@{rds_host}/{rds_db}')
    df.to_sql(name='channels', con=engine, if_exists='replace', index=False)
    connection.commit()
    connection.close()

def playlists_table():
    connection = mysql.connector.connect(
        host=rds_host,
        user=rds_user,
        password=rds_password,
        database=rds_db,
        port=rds_port
    )
    cursor = connection.cursor()
    try:
        create_db_query_2 = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                        Title varchar(80), 
                        ChannelId varchar(100), 
                        ChannelName varchar(100),
                        PublishedAt timestamp,
                        VideoCount int
                        )'''
        cursor.execute(create_db_query_2)
    except:
        st.write("playlists table already created")

    pl_list = []
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df_2 = pd.DataFrame(pl_list)
    engine = create_engine(f'mysql+mysqlconnector://{rds_user}:{rds_password}@{rds_host}/{rds_db}')
    df_2.to_sql(name='playlists', con=engine, if_exists='replace', index=False)
    connection.commit()
    connection.close()

def videos_table():
    connection = mysql.connector.connect(
        host=rds_host,
        user=rds_user,
        password=rds_password,
        database=rds_db,
        port=rds_port
    )
    cursor = connection.cursor()
    try:
        create_db_query_3 = '''create table if not exists videos(
                        Channel_Name varchar(150),
                        Channel_Id varchar(100),
                        Video_Id varchar(50) primary key, 
                        Title varchar(150), 
                        Tags text,
                        Thumbnail varchar(225),
                        Description text, 
                        Published_Date timestamp,
                        Duration time, 
                        Views bigint, 
                        Likes bigint,
                        Comments int,
                        Favorite_Count int,
                        Definition varchar(50),
                        Caption_Status varchar(50)
                        )'''
        cursor.execute(create_db_query_3)
    except:
        st.write("videos table already created")

    vi_list = []
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df_3 = pd.DataFrame(vi_list)
    df_3['Tags'] = df_3['Tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    
    engine = create_engine(f'mysql+mysqlconnector://{rds_user}:{rds_password}@{rds_host}/{rds_db}')
    df_3.to_sql(name='videos', con=engine, if_exists='replace', index=False)
    connection.commit()
    connection.close()


def comments_table():
    connection = mysql.connector.connect(
        host=rds_host,
        user=rds_user,
        password=rds_password,
        database=rds_db,
        port=rds_port
    )
    cursor = connection.cursor()
    try:
        create_db_query_4 = '''create table if not exists comments(
                        Comment_Id varchar(100) primary key,
                        Video_Id varchar(100),
                        Comment_Text text, 
                        Comment_Author varchar(100), 
                        Comment_Published_Date timestamp
                        )'''
        cursor.execute(create_db_query_4)
    except:
        st.write("comments table already created")

    com_list = []
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df_4 = pd.DataFrame(com_list)
    engine = create_engine(f'mysql+mysqlconnector://{rds_user}:{rds_password}@{rds_host}/{rds_db}')
    df_4.to_sql(name='comments', con=engine, if_exists='replace', index=False)
    connection.commit()
    connection.close()

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    st.write("Tables Created successfully")

st.markdown("Create MYSQL Table",help="Click create Tables for RDS Mysql Tables if not created")
if st.button("create Tables"):
    tables()

def migrate_to_mysql():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    st.write("Migration Completed")
st.markdown("Migration to AWS RDS  ",help="Migrating your local Mongodb Nosql datas to RDS Mysql")
if st.button("Migrate to SQL"):
    migrate_to_mysql()

# For showing output in UI
def show_channels_table():
    ch_list = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)
    st.dataframe(df)
    return df

def show_playlists_table():
    pl_list = []
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)
    st.dataframe(df1)
    return df1

def show_videos_table():
    vi_list = []
    coll2 = db["channel_details"]
    for vi_data in coll2.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
    st.dataframe(df2)
    return df2

def show_comments_table():
    com_list = []
    coll3 = db["channel_details"]
    for com_data in coll3.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)
    st.dataframe(df3)
    return df3

with st.sidebar:
    show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

st.header(':violet[Details of Channels]', divider='rainbow')
if show_table == ":green[channels]":
        show_channels_table()
elif show_table == ":orange[playlists]":
        show_playlists_table()
elif show_table ==":red[videos]":
        show_videos_table()
elif show_table == ":blue[comments]":
        show_comments_table()

# For SQL query 
connection = mysql.connector.connect(
        host=rds_host,
        user=rds_user,
        password=rds_password,
        database=rds_db,
        port=rds_port
    )
cursor = connection.cursor()
with st.sidebar:
    st.markdown(
    """
    <style>
    .stRadio label, .stSelectbox label {
        cursor: pointer;
    }
    </style>
    """, unsafe_allow_html=True
)
    question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))

st.header(':blue[Your Queries]', divider='rainbow')

if question=="1. All the videos and the channel name":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)
elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Videos with higest likes":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)
