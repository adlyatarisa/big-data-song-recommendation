import pandas as pd
import json
import time
from kafka import KafkaProducer
import logging

class DatasetStreamer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        self.logger = logging.getLogger(__name__)
    
    def stream_tracks_data(self, csv_path, topic='tracks-stream', batch_size=10):
        """Stream data dari spotify_tracks.csv ke Kafka topic"""
        try:
            # Load CSV dataset
            df = pd.read_csv(csv_path)
            self.logger.info(f"Loaded {len(df)} tracks from {csv_path}")
            
            for index, row in df.iterrows():
                # Convert row to dict berdasarkan kolom yang ada
                track_data = {
                    'track_id': row.get('id', f'track_{index}'),
                    'track_name': row.get('name', ''),
                    'acousticness': row.get('acousticness', 0),
                    'album_id': row.get('album_id', ''),
                    'artists_id': row.get('artists_id', ''),
                    'country': row.get('country', ''),
                    'danceability': row.get('danceability', 0),
                    'duration_ms': row.get('duration_ms', 0),
                    'energy': row.get('energy', 0),
                    'instrumentalness': row.get('instrumentalness', 0),
                    'key': row.get('key', 0),
                    'liveness': row.get('liveness', 0),
                    'loudness': row.get('loudness', 0),
                    'mode': row.get('mode', 0),
                    'popularity': row.get('popularity', 0),
                    'speechiness': row.get('speechiness', 0),
                    'tempo': row.get('tempo', 0),
                    'valence': row.get('valence', 0),
                    'time_signature': row.get('time_signature', 0),
                    'preview_url': row.get('preview_url', ''),
                    'track_number': row.get('track_number', 0),
                    'playlist': row.get('playlist', ''),
                    'timestamp': int(time.time() * 1000)
                }
                
                # Send to Kafka
                self.producer.send(
                    topic, 
                    key=track_data['track_id'],
                    value=track_data
                )
                
                # Batch processing
                if index % batch_size == 0:
                    self.producer.flush()
                    self.logger.info(f"Streamed {index} tracks...")
                    time.sleep(0.1)  # Small delay
            
            self.producer.flush()
            self.logger.info(f"Finished streaming {len(df)} tracks to topic '{topic}'")
            
        except Exception as e:
            self.logger.error(f"Error streaming tracks: {e}")
        finally:
            self.producer.close()
    
    def stream_artists_data(self, csv_path, topic='artists-stream'):
        """Stream data dari spotify_artists.csv"""
        try:
            df = pd.read_csv(csv_path)
            self.logger.info(f"Loaded {len(df)} artists from {csv_path}")
            
            for index, row in df.iterrows():
                artist_data = {
                    'artist_id': row.get('id', f'artist_{index}'),
                    'artist_name': row.get('name', ''),
                    'artist_popularity': row.get('artist_popularity', 0),
                    'followers': row.get('followers', 0),
                    'genres': row.get('genres', '[]'),
                    'track_id': row.get('track_id', ''),
                    'track_name_prev': row.get('track_name_prev', ''),
                    'type': row.get('type', ''),
                    'timestamp': int(time.time() * 1000)
                }
                
                self.producer.send(topic, key=artist_data['artist_id'], value=artist_data)
                
                if index % 10 == 0:
                    self.producer.flush()
                    self.logger.info(f"Streamed {index} artists...")
                    time.sleep(0.05)
            
            self.producer.flush()
            self.logger.info(f"Finished streaming artists to topic '{topic}'")
            
        except Exception as e:
            self.logger.error(f"Error streaming artists: {e}")
    
    def stream_albums_data(self, csv_path, topic='albums-stream'):
        """Stream data dari spotify_albums.csv"""
        try:
            df = pd.read_csv(csv_path)
            self.logger.info(f"Loaded {len(df)} albums from {csv_path}")
            
            for index, row in df.iterrows():
                album_data = {
                    'album_id': row.get('id', f'album_{index}'),
                    'album_name': row.get('name', ''),
                    'album_type': row.get('album_type', ''),
                    'artist_id': row.get('artist_id', ''),
                    'release_date': row.get('release_date', ''),
                    'total_tracks': row.get('total_tracks', 0),
                    'track_id': row.get('track_id', ''),
                    'track_name_prev': row.get('track_name_prev', ''),
                    'images': row.get('images', ''),
                    'external_urls': row.get('external_urls', ''),
                    'timestamp': int(time.time() * 1000)
                }
                
                self.producer.send(topic, key=album_data['album_id'], value=album_data)
                
                if index % 10 == 0:
                    self.producer.flush()
                    self.logger.info(f"Streamed {index} albums...")
                    time.sleep(0.05)
            
            self.producer.flush()
            self.logger.info(f"Finished streaming albums to topic '{topic}'")
            
        except Exception as e:
            self.logger.error(f"Error streaming albums: {e}")
    
    def stream_emotion_data(self, json_path, topic='emotion-stream'):
        """Stream data dari emotion JSON file"""
        try:
            with open(json_path, 'r') as f:
                # Baca file JSON line by line karena formatnya JSONL (JSON Lines)
                emotion_data = []
                for line in f:
                    if line.strip():  # Skip empty lines
                        emotion_data.append(json.loads(line))
        
            self.logger.info(f"Loaded {len(emotion_data)} emotion records from {json_path}")
            
            for index, record in enumerate(emotion_data):
                # Map fields sesuai dengan struktur JSON yang ada
                emotion_record = {
                    'song_id': f'emotion_song_{index}',  # Generate ID karena ga ada di JSON
                    'song_name': record.get('song', ''),
                    'artist_name': record.get('artist', ''),
                    'emotion': record.get('emotion', ''),
                    'variance': record.get('variance', 0),
                    'genre': record.get('Genre', ''),
                    'release_date': record.get('Release Date', 0),
                    'key': record.get('Key', ''),
                    'tempo': record.get('Tempo', 0),
                    'loudness': record.get('Loudness', 0),
                    'explicit': record.get('Explicit', ''),
                    'popularity': record.get('Popularity', 0),
                    'energy': record.get('Energy', 0),
                    'danceability': record.get('Danceability', 0),
                    'positiveness': record.get('Positiveness', 0),
                    'speechiness': record.get('Speechiness', 0),
                    'liveness': record.get('Liveness', 0),
                    'acousticness': record.get('Acousticness', 0),
                    'instrumentalness': record.get('Instrumentalness', 0),
                    'timestamp': int(time.time() * 1000)
                }
                
                self.producer.send(
                    topic, 
                    key=emotion_record['song_id'], 
                    value=emotion_record
                )
                
                if index % 10 == 0:
                    self.producer.flush()
                    self.logger.info(f"Streamed {index} emotion records...")
                    time.sleep(0.05)
        
            self.producer.flush()
            self.logger.info(f"Finished streaming emotion data to topic '{topic}'")
            
        except Exception as e:
            self.logger.error(f"Error streaming emotion data: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    streamer = DatasetStreamer()
    
    # Stream all datasets
    print("Starting data streaming...")
    
    # Stream tracks data (structured)
    streamer.stream_tracks_data('data/raw/spotify_tracks.csv')
    
    # Stream artists data (structured)
    streamer.stream_artists_data('data/raw/spotify_artists.csv')
    
    # Stream albums data (structured)
    streamer.stream_albums_data('data/raw/spotify_albums.csv')
    
    # Stream emotion data (semi-structured JSON)
    streamer.stream_emotion_data('data/raw/song_emotion_data.json')
    
    print("All streaming completed!")