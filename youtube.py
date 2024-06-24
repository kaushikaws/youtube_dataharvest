
from googleapiclient.discovery import build
from pymongo import MongoClient
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import errorcode
#API key connection
 
def Api_connect():
    Api_Id = "AIzaSyBXy02RuPxzfbnML_UdH5vSL_UBLMILpJ0"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube
youtube = Api_connect()

#get channels information

def get_channel_info(channel_id):
    request=youtube.channels().list(
                                part="snippet,ContentDetails,statistics",
                                                    id=channel_id
                                                        )
    response = request.execute()
    for i in response['items']:
        data = dict(Channel_Name = i["snippet"]["title"],
                    Channel_Id=i["id"],Subscribers=i['statistics']['subscriberCount'],
                    Views=i["statistics"]["viewCount"],
                    Total_Videos=i["statistics"]["videoCount"],
                    Channel_Description=i["snippet"]["description"],
                    Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return data
channel_details = get_channel_info("UCctcLdajnkxHT-WziLGc5fA")

#get video ids

def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,part = 'contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    while True:
         response1=youtube.playlistItems().list(part='snippet',playlistId=Playlist_Id,maxResults=50,
                                                pageToken=next_page_token).execute()
         for i in range(len(response1['items'])):
             video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
         next_page_token=response1.get('nextPageToken')
         if next_page_token is None:
             break
    return video_ids
video_Ids = get_videos_ids("UCctcLdajnkxHT-WziLGc5fA")

#get video info
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data
video_info = get_video_info(video_Ids)

#get comment info
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

comment_info= get_comment_info(video_Ids)

#get playlist info

def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(part='snippet,contentDetails',
                                         channelId=channel_id,
                                         maxResults=50,
                                         pageToken=next_page_token)
        response = request.execute()
        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                      Title=item['snippet']['title'],
                      Channel_Id=item['snippet']['channelId'],
                      Channel_Name=item['snippet']['channelTitle'],
                      PublishedAt=item['snippet']['publishedAt'],
                      Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data
play = get_playlist_details("UCctcLdajnkxHT-WziLGc5fA")

#upload channel datas to local Mongo DB

connection_string = "localhost:27017"
client = MongoClient(connection_string)
db = client["youtube"]
coll = db["channel_details"]
def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"

mongo_db = channel_details("UCctcLdajnkxHT-WziLGc5fA")

# creating tables in mySQL

rds_user = 'admin'
rds_password = 'administrator'
rds_host = 'database.cd6808ymyl01.ap-south-1.rds.amazonaws.com'
rds_port = '3306'
rds_db = 'youtube'
try:
    connection = mysql.connector.connect(
        host=rds_host,
        user=rds_user,
        password=rds_password,
        database=rds_db,
        port = rds_port
    )
    cursor = connection.cursor()
    print("Connected to the RDS MySQL instance successfully")
    
    create_db_query = """ create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))"""
    qr = cursor.execute(create_db_query)
    create_db_query_2 = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                        Title varchar(80), 
                        ChannelId varchar(100), 
                        ChannelName varchar(100),
                        PublishedAt timestamp,
                        VideoCount int
                        )'''
    qr2 = cursor.execute(create_db_query_2)
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
                        Definition varchar(10), 
                        Caption_Status varchar(50) 
                        )''' 
    qr3 = cursor.execute(create_db_query_3)
    create_db_query4 = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(50),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published timestamp)
                       '''
    qr4 = cursor.execute(create_db_query4)   
except:
    print("Error creating Tables")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")

# convering nosql  to pandas DataFrame
# channel list
ch_list = []
db = client["youtube"]
coll1 = db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
    ch_list.append(ch_data["channel_information"])
df = pd.DataFrame(ch_list)
#playlist
pl_list = []
db = client["youtube"]
coll1 = db["channel_details"]
for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
    for i in range(len(pl_data["playlist_information"])):
        pl_list.append(pl_data["playlist_information"][i])
df_2 = pd.DataFrame(pl_list)
#videos
vi_list = []
db = client["youtube"]
coll1 = db["channel_details"]
for vi_data in coll1.find({},{"_id":0,"video_information":1}):
    for i in range(len(vi_data["video_information"])):
        vi_list.append(vi_data["video_information"][i])
df_3 = pd.DataFrame(vi_list)
df_3['Tags'] = df_3['Tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
#comments
com_list = []
db = client["youtube"]
coll1 = db["channel_details"]
for com_data in coll1.find({},{"_id":0,"comment_information":1}):
    for i in range(len(com_data["comment_information"])):
        com_list.append(com_data["comment_information"][i])
df_4 = pd.DataFrame(com_list)

#upload datas to mysql 
mysql_conn = mysql.connector.connect(
    host= rds_host,
    user=rds_user,
    password=rds_password,
    database=rds_db
)

# Create SQLAlchemy engine
engine = create_engine(f'mysql+mysqlconnector://{rds_user}:{rds_password}@{rds_host}/{rds_db}')
table = "youtube"
# DataFrame to MySQL
table_name_1 = 'channels'
df.to_sql(name=table_name_1, con=engine, if_exists='replace', index=False )
table_name_2 = 'playlists'
df_2.to_sql(name=table_name_2, con=engine, if_exists='replace', index=False )
table_name_4 = 'comments'
df_4.to_sql(name=table_name_4, con=engine, if_exists='replace', index=False )
table_name_3 = 'videos'
df_3.to_sql(name=table_name_3, con=engine, if_exists='replace', index=False )

# Commit the transaction
mysql_conn.commit()

# Close connections
mysql_conn.close()

print(f"DataFrame uploaded to MySQL RDS table '{table}' successfully.")

#streamlit
import streamlit as st
# creating sidebar

with st.sidebar:
    st.title(":red[Youtube DH]")
    st.header("Skills")
    st.caption("python Scripting")
    st.caption("Data Collection")
    st.caption("API")
    st.caption("MongoDB")
    st.caption("MYSQL")

#input to enter channel ID

channel_id = st.text_input("Enter Channel ID")

if st.button("collect and store data"):
    ch_ids = []
    db = client["youtube"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    if channel_id in ch_ids:
        st.sidebar.warning("CHANNEL DETAILS OF THE GIVEN CHANNEL ID ALREADY EXISTS ")
    else:
        insert = channel_details(channel_id)
        st.sidebar.success(insert)
st.sidebar.subheader("Table Migration")
if st.sidebar.button("MIGRATE TO POSTGRESQL"):
    Table = db.client
    st.sidebar.success(Table)
