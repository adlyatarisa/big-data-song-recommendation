import logging
import threading
import time
import sys
import os

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from streaming.kafka_producer import DatasetStreamer
from streaming.kafka_consumer import DataConsumer

def run_producer():
    """Run producer dalam thread terpisah"""
    print("🚀 Starting producer...")
    streamer = DatasetStreamer()
    
    try:
        # Stream tracks data
        print("📀 Streaming tracks data...")
        streamer.stream_tracks_data('../data/raw/spotify_tracks.csv')
        
        time.sleep(2)
        
        # Stream artists data
        print("🎤 Streaming artists data...")
        streamer.stream_artists_data('../data/raw/spotify_artists.csv')
        
        time.sleep(2)
        
        # Stream albums data  
        print("💿 Streaming albums data...")
        streamer.stream_albums_data('../data/raw/spotify_albums.csv')
        
        time.sleep(2)
        
        # Stream emotion data - UPDATE NAMA FILE INI
        print("😊 Streaming emotion data...")
        streamer.stream_emotion_data('../data/raw/NAMA_FILE_JSON_KAMU.json')  # ← GANTI INI
        
        print("✅ Producer finished!")
        
    except Exception as e:
        print(f"❌ Producer error: {e}")

def run_consumer():
    """Run consumer dalam thread terpisah"""
    print("🔄 Starting consumer...")
    consumer = DataConsumer()
    consumer.consume_and_store()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("🎵 Starting Big Data Song Recommendation Streaming...")
    
    # Start consumer first
    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    consumer_thread.start()
    print("✅ Consumer started")
    
    # Wait a bit then start producer
    time.sleep(5)
    producer_thread = threading.Thread(target=run_producer)
    producer_thread.start()
    print("✅ Producer started")
    
    # Keep main thread alive
    try:
        while producer_thread.is_alive():
            time.sleep(1)
        print("🎉 Streaming completed successfully!")
    except KeyboardInterrupt:
        print("⏹️ Shutting down...")