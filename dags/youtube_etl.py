from googleapiclient.discovery import build
# from pymongo import MongoClient
import pandas as pd
import re
from sqlalchemy import create_engine
from datetime import timedelta
import psycopg2

# def connect_db(database, collection):
# 	CONNECTION_STRING = "mongodb+srv://tanduy2407:tanduy240797@duncanpham.t4xoc.mongodb.net/?retryWrites=true&w=majority"
# 	client = MongoClient(CONNECTION_STRING)
# 	dbname = client[database]
# 	collection_name = dbname[collection]
# 	return collection_name


def get_data():
	api_key = 'AIzaSyDnoUaW29xRNWmHwzGxeur1xTrntzFfB7Y'
	youtube = build('youtube', 'v3', developerKey=api_key,
					cache_discovery=False)

   # get channel id
	request = youtube.channels().list(
		part="snippet,contentDetails,statistics",
		forUsername='schafer5')
	ch_response = request.execute()
	channel_id = ch_response['items'][0]['id']

	request = youtube.playlists().list(
		part="snippet,contentDetails",
		channelId=channel_id)
	response = request.execute()
	num_playlist = response['pageInfo']['totalResults']

	# get playlist for channel
	pl_request = youtube.playlists().list(
		part="snippet,contentDetails",
		channelId=channel_id,
		maxResults=num_playlist)
	pl_response = pl_request.execute()

	playlist_ids = []
	item_count = []
	playlist_title = []
	for item in pl_response['items']:
		playlist_ids.append(item['id'])
		item_count.append(item['contentDetails']['itemCount'])
		playlist_title.append(item['snippet']['title'])
	all_dict_data = []
	values = []
	hours_pattern = re.compile(r'(\d+)H')
	minutes_pattern = re.compile(r'(\d+)M')
	second_pattern = re.compile(r'(\d+)S')

	# response for all videos in each playlists
	for id, count, pl_title in zip(playlist_ids, item_count, playlist_title):
		request = youtube.playlistItems().list(
			part="snippet,contentDetails",
			playlistId=id,
			maxResults=count)
		response = request.execute()

		# loop through each video in 1 playlist to get information
		for vid in response['items']:
			video_id = vid['contentDetails']['videoId']
			vd_request = youtube.videos().list(
				part="snippet,contentDetails,statistics",
				id=video_id)
			vd_response = vd_request.execute()

			title = vd_response['items'][0]['snippet']['title']
			publishedDate = vd_response['items'][0]['snippet']['publishedAt'][:10]
			duration = vd_response['items'][0]['contentDetails']['duration']
			description = vd_response['items'][0]['snippet']['description']
			hours = hours_pattern.search(duration)
			minutes = minutes_pattern.search(duration)
			seconds = second_pattern.search(duration)

			hours = int(hours.group(1)) if hours else 0
			minutes = int(minutes.group(1)) if minutes else 0
			seconds = int(seconds.group(1)) if seconds else 0
			duration = int(timedelta(
				hours=hours, minutes=minutes, seconds=seconds).total_seconds())

			likeCount = int(vd_response['items'][0]['statistics']['likeCount'])
			viewCount = int(vd_response['items'][0]['statistics']['viewCount'])
			commentCount = int(
				vd_response['items'][0]['statistics']['commentCount'])
			dict_data = {'video_id': video_id, 'playlist': pl_title, 'title': title, 'publishedDate': publishedDate, 'desc': description,
						 'duration': duration, 'likeCount': likeCount, 'viewCount': viewCount, 'commentCount': commentCount}
			all_dict_data.append(dict_data)
			tuple_data = (video_id, pl_title, title, publishedDate, description,
						 duration, likeCount, viewCount, commentCount)
			values.append(tuple_data)
	return values


# def query_to_mongodb(database, collection, method, data=None, key=None, value=None,
# 					 key_query=None, value_to_query=None, key_update=None, value_to_update=None):
# 	collection_name = connect_db(database, collection)
# 	if method == 'insert' and data is not None:
# 		collection_name.insert_many(data)

# 	if method == 'select':
# 		doc = collection_name.find({key: value})
# 		df = pd.DataFrame(list(doc))
# 		return doc

# 	if method == 'update' and key_query is not None:
# 		collection_name.update_many({key_query: value_to_query},
# 									{'$set': {key_update: value_to_update}})


def query_to_postgres(keyword, values=None):
	conn = psycopg2.connect(database="airflow", user='airflow',
							password='airflow', host='postgres', port='5432')
	cursor = conn.cursor()
	if keyword == 'create':
		cursor.execute("""
			CREATE TABLE IF NOT EXISTS youtube_data
			(video_id VARCHAR(50),
			playlist_title VARCHAR(200),
			video_title VARCHAR(200),
			published_date DATE NOT NULL,
			description VARCHAR(5000),
			duration INT,
			like_count INT,
			view_count INT,
			comment_count INT)
			""")
		print("Table created successfully........")

	if keyword == 'insert' and values:
		print(values)
		cursor.executemany("INSERT INTO youtube_data VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)", values)
		print(f'{cursor.rowcount} inserted')
		
	cursor.close()
	conn.commit()
	conn.close()



# if __name__ == "__main__":
# 	values = get_data()
# 	print(values)
	# pprint.pprint(data)
	# query('youtube_api', 'data', 'insert', data)

	# query('youtube_api', 'data', 'update', key_query='video_id',value_to_query='HlLK5BA0wT0', key_update='duration', value_to_update=80)

	# query('youtube_api', 'data', 'select', key='playlist', value='Pandas Tutorials')
