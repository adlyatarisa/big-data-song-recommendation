import json
import pandas as pd
from kafka import KafkaConsumer
from storage.minio_client import MinIOClient  # Remove 'src.'
import logging
from datetime import datetime

class DataConsumer:
    def __init__(self, bootstrap_servers='kafka:29092'):  # Use Docker service name
        self.consumer = KafkaConsumer(
            'tracks-stream',
            'artists-stream', 
            'albums-stream',
            'emotion-stream',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8'),
            group_id='recommendation-system'
        )
        self.minio_client = MinIOClient(endpoint='minio:9000')  # Use Docker service name
        
        # Buffers untuk setiap data type
        self.tracks_buffer = []
        self.artists_buffer = []
        self.albums_buffer = []
        self.emotion_buffer = []
        self.buffer_size = 50
        self.logger = logging.getLogger(__name__)
    
    def consume_and_store(self):
        """Consume data dari Kafka dan store ke MinIO"""
        try:
            for message in self.consumer:
                topic = message.topic
                data = message.value
                
                if topic == 'tracks-stream':
                    self.tracks_buffer.append(data)
                    if len(self.tracks_buffer) >= self.buffer_size:
                        self._store_tracks_batch()
                        
                elif topic == 'artists-stream':
                    self.artists_buffer.append(data)
                    if len(self.artists_buffer) >= self.buffer_size:
                        self._store_artists_batch()
                        
                elif topic == 'albums-stream':
                    self.albums_buffer.append(data)
                    if len(self.albums_buffer) >= self.buffer_size:
                        self._store_albums_batch()
                        
                elif topic == 'emotion-stream':
                    self.emotion_buffer.append(data)
                    if len(self.emotion_buffer) >= self.buffer_size:
                        self._store_emotion_batch()
                
                self.logger.info(f"Processed message from {topic}")
                
        except KeyboardInterrupt:
            self.logger.info("Stopping consumer...")
            # Store remaining data
            self._store_all_remaining()
        finally:
            self.consumer.close()
    
    def _store_tracks_batch(self):
        if not self.tracks_buffer:
            return
            
        df = pd.DataFrame(self.tracks_buffer)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"tracks_batch_{timestamp}.csv"
        
        # Update path untuk relative dari src folder
        local_path = f"../data/processed/{filename}"
        df.to_csv(local_path, index=False)
        
        self.minio_client.upload_file('tracks-data', local_path, filename)
        self.logger.info(f"Stored {len(self.tracks_buffer)} tracks to MinIO")
        self.tracks_buffer.clear()
    
    def _store_artists_batch(self):
        if not self.artists_buffer:
            return
            
        df = pd.DataFrame(self.artists_buffer)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"artists_batch_{timestamp}.csv"
        
        local_path = f"../data/processed/{filename}"
        df.to_csv(local_path, index=False)
        
        self.minio_client.upload_file('artists-data', local_path, filename)
        self.logger.info(f"Stored {len(self.artists_buffer)} artists to MinIO")
        self.artists_buffer.clear()
    
    def _store_albums_batch(self):
        if not self.albums_buffer:
            return
            
        df = pd.DataFrame(self.albums_buffer)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"albums_batch_{timestamp}.csv"
        
        local_path = f"../data/processed/{filename}"
        df.to_csv(local_path, index=False)
        
        self.minio_client.upload_file('albums-data', local_path, filename)
        self.logger.info(f"Stored {len(self.albums_buffer)} albums to MinIO")
        self.albums_buffer.clear()
    
    def _store_emotion_batch(self):
        if not self.emotion_buffer:
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"emotion_batch_{timestamp}.json"
        
        local_path = f"../data/processed/{filename}"
        with open(local_path, 'w') as f:
            json.dump(self.emotion_buffer, f, indent=2)
    
        self.minio_client.upload_file('emotion-data', local_path, filename)
        self.logger.info(f"Stored {len(self.emotion_buffer)} emotion records to MinIO")
        self.emotion_buffer.clear()
    
    def _store_all_remaining(self):
        """Store semua buffer yang masih ada"""
        if self.tracks_buffer:
            self._store_tracks_batch()
        if self.artists_buffer:
            self._store_artists_batch()
        if self.albums_buffer:
            self._store_albums_batch()
        if self.emotion_buffer:
            self._store_emotion_batch()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    consumer = DataConsumer()
    consumer.consume_and_store()