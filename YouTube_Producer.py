from urllib.error import HTTPError
from kafka import KafkaProducer
from googleapiclient.discovery import build
import json
import time

# Creating a Kafka producer
youTube_producer = KafkaProducer(bootstrap_servers='localhost:9092')

# create a topic
topic_name = "youtube-search-data"

# reading the key file from config
key_file = open('./config')
lines = key_file.readlines()

#Google API service name and version
api_service_name = "youtube"
api_version = "v3"

# Creating youtube build object for search and video data
youtube_search = build(
                api_service_name, 
                api_version, 
                developerKey=lines[1]
            )
youtube_video = build(
                api_service_name, 
                api_version, 
                developerKey=lines[0]
            )

# Getting video metadata from youtube API like id, title, description, published date, category, tags, etc
def video_data(video_id):
    try:
        # Getting video data from youtube API
        request = youtube_video.videos()\
                            .list(
                                id=video_id,
                                part="id,snippet,contentDetails,statistics"                                
                            )

        # Executing the request
        response = request.execute()

    # exception on HTTP error
    except HTTPError:
        pass

    return response

# main method to get video data using search API with latest uploads
def main():
    #Searching video Ids from youtube API
    try:
        request = youtube_search.search()\
                            .list(
                                part="snippet",
                                maxResults=50,
                                type="video",
                                relevanceLanguage="en",
                                order="date",
                                videoDuration="long",
                            )
        # Executing the request
        response = request.execute()
        # Iterating over the response to get video Ids and sending to Kafka
        for idx in response['items']:           
            video_id = idx['id']['videoId']
            print(video_id)
            result = video_data(video_id)
            response = json.dumps(result)
            youTube_producer.send(topic_name, response.encode('utf-8'))

    # exception on HTTP error
    except HTTPError:
        pass

if __name__ == "__main__":
    # Calling youtube search API for 10000 times and sleeping for 300 seconds. Youtube API has a limit of 10000 requests per day.
    for i in range(0, 10000):
        response = main()        
        time.sleep(300)
    