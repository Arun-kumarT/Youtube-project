import googleapiclient.discovery
import mysql.connector
import pandas as pd
import pymongo
from datetime import datetime
from dateutil import parser
import streamlit as st

#api
api_key='AIzaSyBeXArx44kSoWt3RCwEXg9iM2ATa2X9u3U'
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

#channel details
def channel_details(channel_id):
  request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= channel_id
    )
  response = request.execute()

  b = dict(channel_name = response['items'][0]['snippet']['title'],
           channel_id = response['items'][0]['id'],
           description = response['items'][0]['snippet']['description'],
           joined = response['items'][0]['snippet']['publishedAt'],
           thumbnail = response['items'][0]['snippet']['thumbnails']['medium']['url'],
           subscriberCount = response['items'][0]['statistics']['subscriberCount'],
           videoCount = response['items'][0]['statistics']['videoCount'],
           total_views = response['items'][0]['statistics']['viewCount'],
           playlist_id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
           )
  return b

#video id
def video_ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(
            part="contentDetails",
            id= channel_id).execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
        
    return video_ids

#duration to sec 
def durationInSeconds(duration):
    duration =list(duration)
    del duration[0:2]
    duration_seconds = 0
  #enumerate is used because this builtin function can able to Returns an iterator with index and element pairs from the original iterable
   #x += 5, Equivalent to x = x + 5
    for i,e in enumerate(duration):
        if(e == 'H'):
            duration_seconds += int(duration[i-1])* 60 * 60
        elif(e == 'M'):
            duration_seconds += int(duration[i-1])* 60
        elif(e == 'S'):
            duration_seconds += int(duration[i-1])
    return duration_seconds

#Changing Date format
# function is used to parse date strings in ISO 8601 format.
def changeDateFormat(date_string):
    datetime_obj = parser.isoparse(date_string)
    format_datetime = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
    return format_datetime


#get video information

def video_info(all_video_id):
    video_data=[]
    for video_id in all_video_id:
        request=youtube.videos().list(
            part='snippet,ContentDetails,statistics',
            id=video_id
        )
        response=request.execute()

        for item in response['items']:
            data=dict(channel_Name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    title=item['snippet']['title'],
                    thumbnails=item['snippet']['thumbnails']['default']['url'],
                    description=item['snippet'].get('description'),
                    published_date=item['snippet']['publishedAt'],
                    duration=item['contentDetails']['duration'],
                    likes=item['statistics'].get('likeCount'),
                    views=item['statistics'].get('viewCount'),
                    comments=item['statistics'].get('commentCount')
                    )
            date_string = data['published_date'] 
            date_string = changeDateFormat(date_string)
            data['published_date'] = date_string
            duration = data['duration']
            duration = durationInSeconds(duration)
            data['duration'] = duration
            video_data.append(data)
            video_data.append(data)
    return video_data

#comments_info
def comment_info(all_video_id):
    comment_data=[]
    #try function used - in case of comment disabled error occur so if error occur,
    #we have to call next video comments
    try:
        for video_id in all_video_id:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50,
            )
            response=request.execute()

            for item in response['items']:
                data=dict(
                    comment_id=item['snippet']['topLevelComment']['id'],
                    video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(data)
    except:
            pass
    return comment_data

#playlist_details
def playlist_details(channel_id):
    next_page_token=None
    playlist_data=[]

    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(
                playlist_id=item['id'],
                title=item['snippet']['title'],
                channel_id=item['snippet']['channelId'],
                channel_name=item['snippet']['channelTitle'],
                published_date=item['snippet']['publishedAt'],
                video_count=item['contentDetails']['itemCount']
            )
            playlist_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data
#streamlit code
#project Header
st.header('YouTube Data Harvesting and Warehousing using SQL and Streamlit  ', divider='rainbow')  
st.subheader('                      By ARUNKUMAR T:sunglasses:')
 
channel_id= st.text_input('Enter channel_id:',label_visibility = 'collapsed') 
if  st.button('scrape') and channel_id:
  st.write('Preview',channel_details(channel_id))

#mysql and Mongodb connection
mydb= mysql.connector.connect(
                        host='localhost',
                        user='root',
                        password="T@run1797",
                        database="youtubedb1",
                        port=3306
    )
mycursor = mydb.cursor()

client = pymongo.MongoClient("mongodb://localhost:27017/")
db=client['youtube_data']

def channel_info(channel_id):
    Ch_details=channel_details(channel_id)
    playlist_info=playlist_details(channel_id)
    all_video_id=video_ids(channel_id)
    video_details=video_info(all_video_id)
    comment_details=comment_info(all_video_id)

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db=client['youtube_data']
    collection=db['channel_info']
    collection.insert_one({'channel_information':Ch_details,'playlist_information':playlist_info,
                            'video_information':video_details,'comment_information':comment_details})


#store data in Mongodb

if st.button("collect and store data"):
    channel_ids=[]
    db=client['youtube_data']
    collection=db['channel_info']
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        channel_ids.append(ch_data["channel_information"]["channel_id"])

    if channel_id in channel_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_info(channel_id)
        st.write(insert,'Inserted')
        

#creating MySQL table
create_table='''create table if not exists channel_details(
                        channel_name VARCHAR(40),
                        channel_id VARCHAR(25) primary key,
                        description varchar(1000),
                        joined varchar(1000),
                        thumbnail varchar(1000),
                        subscriberCount int,
                        videoCount int,
                        total_views int,
                        playlist_id varchar(50))'''
mycursor.execute(create_table)
mydb.commit()

create_table='''create table if not exists playlist(
                        playlist_id varchar(100) primary key,
                        title varchar(100),
                        channel_id varchar(100),
                        channel_name varchar(100),
                        published_date varchar(100),
                        video_count int)'''
mycursor.execute(create_table)
mydb.commit()

create_table='''create table if not exists videos(
                    channel_Name varchar(50),
                    channel_id varchar(50),
                    Video_Id varchar(50) primary key,
                    title varchar(100),
                    thumbnails varchar(100),
                    description varchar(500),
                    published_date varchar(50),
                    duration varchar(50),
                    likes int,
                    views int,
                    comments int)'''
mycursor.execute(create_table)
mydb.commit()

create_table='''create table if not exists comments(
                    comment_id varchar(50),
                    video_id varchar(50) primary key,
                    comment_text varchar(5000),
                    comment_author varchar(50),
                    comment_published varchar(50))'''
mycursor.execute(create_table)
mydb.commit()


#channel details table
def channel_details():
    db=client['youtube_data']
    collection=db['channel_info']
    d=collection.find_one({'channel_information.channel_id':channel_id})
    channel_list=d['channel_information']
    dct = {k:[v] for k,v in channel_list.items()}
    df=pd.DataFrame(dct)
    df1=df.to_records().tolist()
    for i in df1:
        
        sql='''insert ignore into channel_details(
                        channel_name,channel_id,description,
                        joined,thumbnail,subscriberCount,
                        videoCount,total_views,playlist_id)
                            values (%s,%s,%s,%s,%s,%s,%s,%s,%s)''' 
        mycursor.execute(sql,i[1:])
    mydb.commit()

#playlist table
def playlist_details():
        db=client['youtube_data']
        collection=db['channel_info']
        d=collection.find_one({'channel_information.channel_id':channel_id})
        playlist=d['playlist_information']
        df=pd.DataFrame(playlist)
        df1=[]
        for i in df.to_records(index=False).tolist():
                df1.append(i)
        for i in df1:
                sql='''insert ignore into playlist(
                                        playlist_id,
                                        title,
                                        channel_id,
                                        channel_name,
                                        published_date,
                                        video_count)
                                values (%s,%s,%s,%s,%s,%s)''' 
                mycursor.execute(sql,i)
        mydb.commit()

#video details table
def video_data():
        db=client['youtube_data']
        collection=db['channel_info']
        d=collection.find_one({'channel_information.channel_id':channel_id})
        videolist=d['video_information']
        df=pd.DataFrame(videolist)
        df1=[]
        for i in df.to_records(index=False).tolist():
                df1.append(i)
        for j in df1:
                sql='''insert ignore into videos(
                                channel_Name,
                                channel_id,
                                Video_Id,
                                title,
                                thumbnails,
                                description,
                                published_date,
                                duration,
                                likes,
                                views,
                                comments)
                                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''' 
                mycursor.execute(sql,j)
                
        mydb.commit()

#comment details table
def commentdata():  
        db=client['youtube_data']
        collection=db['channel_info'] 
        d=collection.find_one({'channel_information.channel_id':channel_id})
        commentlist=d['comment_information']
        df=pd.DataFrame(commentlist)
        df1=[]
        for i in df.to_records(index=False).tolist():
                df1.append(i)
        for j in df1:
                sql='''insert ignore into comments(
                        comment_id,
                        video_id ,
                        comment_text,
                        comment_author,
                        comment_published)
                        values(%s,%s,%s,%s,%s)''' 
                mycursor.execute(sql,j)
        mydb.commit()

def tables():
    channel_details()
    playlist_details()
    video_data()
    commentdata()

    return "Tables created successfully"

#migration of data to Mysql
if st.button("Migrate to Sql"):
    Table=tables()
    st.success(Table)

# display channel dataframe 
def show_channels_table():
    ch_list=[]
    db=client['youtube_data']
    collection=db['channel_info']
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

# display playlist dataframe 
def show_playlist_table():
    ch_list=[]
    db=client['youtube_data']
    collection=db['channel_info']
    for pl_data in collection.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            ch_list.append(pl_data['playlist_information'][i])

    df=pd.DataFrame(ch_list)
    df1=df.to_records(index=False).tolist()
    df3=st.dataframe(df1)
    
    
    return df3

# display video dataframe 
def show_video_table():
    ch_list=[]
    db=client['youtube_data']
    collection=db['channel_info']
    for pl_data in collection.find({},{'_id':0,'video_information':1}):
        for i in range(len(pl_data['video_information'])):
            ch_list.append(pl_data['video_information'][i])

    df=pd.DataFrame(ch_list)
    df1=df.to_records(index=False).tolist()
    df3=st.dataframe(df1)

    return df3

# display comment dataframe 
def show_comment_table():
    ch_list=[]
    db=client['youtube_data']
    collection=db['channel_info']
    for pl_data in collection.find({},{'_id':0,'comment_information':1}):
        for i in range(len(pl_data['comment_information'])):
            ch_list.append(pl_data['comment_information'][i])

    df=pd.DataFrame(ch_list)
    df1=df.to_records(index=False).tolist()
    df3=st.dataframe(df1)

    return df3

show_table=st.radio("SELECT WHICH TABLE TO VIEW",("None","CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))
if show_table=="None":
    print('select table')
elif show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlist_table()

elif show_table=="VIDEOS":
    show_video_table()

elif show_table=="COMMENTS":
    show_comment_table()




question=st.selectbox("Select your question",("None",
                                              "1. What are the names of all the videos and their corresponding channels?",
                                              "2. Which channels have most number of videos, and how many videos?",
                                              "3. What are the top 10 most viewed videos and their respective channels?",
                                              "4. How many comments were made on each video?",
                                              "5. Which videos have the highest number of likes and corresponding channel name?",
                                              "6. Total number of likes and dislikes for each video, and corresponding video names?",
                                              "7. Total number of views for each channel and corresponding channel names",
                                              "8. Names of all the channels that have published videos in the year 2022",
                                              "9. What is the average duration of all videos in each channel and corresponding channel names?",
                                              "10.Which videos have the highest number of comments and corresponding channel names?"))
if question=="None":
  print()
elif question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''SELECT title, channel_Name FROM videos;'''
    mycursor.execute(query1)
    Q1 = mycursor.fetchall()
    df1=pd.DataFrame(Q1,columns=["video title","channel name"])
    st.write(df1)

elif question=="2. Which channels have most number of videos, and how many videos?":
    query2='''SELECT channel_name as ChannelName,videoCount as  TotalVideos from channel_details order by videoCount desc;'''
    mycursor.execute(query2)
    Q2 = mycursor.fetchall()
    df2=pd.DataFrame(Q2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3='''select views as views,channel_Name as channelName,title as Videotitle from videos 
                where views is not null order by views desc;'''
    mycursor.execute(query3)
    Q3 = mycursor.fetchall()
    df3=pd.DataFrame(Q3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. How many comments were made on each video?":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null;'''
    mycursor.execute(query4)
    Q4 = mycursor.fetchall()
    df4=pd.DataFrame(Q4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Which videos have the highest number of likes and corresponding channel name?":
    query5='''select title as videotitle,channel_Name as channelname,likes as LikeCount
                from videos where likes is not null order by likes desc;'''
    mycursor.execute(query5)
    Q5 = mycursor.fetchall()
    df5=pd.DataFrame(Q5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. Total number of likes and dislikes for each video, and corresponding video names?":
    query6='''select likes as LikeCount,title as Videotitle from videos;'''
    mycursor.execute(query6)
    Q6 = mycursor.fetchall()
    df6=pd.DataFrame(Q6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. Total number of views for each channel and corresponding channel names":
    query7='''select channel_name as ChannelName ,total_views as Totalviews from channel_details;'''
    mycursor.execute(query7)
    Q7 = mycursor.fetchall()
    df7=pd.DataFrame(Q7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. Names of all the channels that have published videos in the year 2022":
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022;'''
    mycursor.execute(query8)
    Q8 = mycursor.fetchall()
    df8=pd.DataFrame(Q8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. What is the average duration of all videos in each channel and corresponding channel names?":
    query9='''select channel_Name as channelname,AVG(duration) as averageduration from videos group by channel_name;'''
    mycursor.execute(query9)
    Q9 = mycursor.fetchall()
    df9=pd.DataFrame(Q9,columns=["channelname","average duration in sec"])
    st.write(df9)
    

elif question=="10.Which videos have the highest number of comments and corresponding channel names?":
    query10='''select title as videotitle, channel_Name as ChannelName,comments as comments from videos where comments is
                not null order by comments desc;'''
    mycursor.execute(query10)
    Q10 = mycursor.fetchall()
    df10=pd.DataFrame(Q10,columns=["video title","channel name","comments"])
    st.write(df10)



