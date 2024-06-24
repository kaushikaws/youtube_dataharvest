
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

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"
    
def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    return df

def show_playlists_table():
    db = client["Youtube_data"]
    coll1 =db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)
    return df1

def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)
    return df2

def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)
    return df3

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

    
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)
            
if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlists_table()
elif show_table ==":red[videos]":
    show_videos_table()
elif show_table == ":blue[comments]":
    show_comments_table()

#SQL connection
mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="admin",
            database= "youtube_data",
            port = "5432"
            )
cursor = mydb.cursor()
    
question = st.selectbox(
    'Please Select Your Question',
    ('1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

#Q1
if question == '1. What are the names of all the videos and their corresponding channels?':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

#Q2
elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

#Q3
elif question == '3. What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

#Q4
elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

#Q5
elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

#Q6
elif question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

#Q7
elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

#Q8
elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

#Q9
elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

#Q10
elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
