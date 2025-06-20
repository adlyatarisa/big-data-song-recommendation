import json
import time
import random
import os
import pandas as pd
from datetime import datetime, timedelta
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MusicStreamingService:
    def __init__(self):
        # Load real datasets
        self.tracks_df = None
        self.users_sample = None
        self.track_features = {}
        
        # Kafka setup (optional)
        self.producer = None
        
        # Batch configuration
        self.batch_size = 50
        self.batch_interval = 60
        self.current_batch = []
        self.batch_counter = 0
        
        # Storage paths
        self.batch_storage_path = "/app/data/batches"
        self.raw_batch_path = f"{self.batch_storage_path}/raw"
        self.processed_batch_path = f"{self.batch_storage_path}/processed"
        
        self.setup_directories()
        self.load_real_datasets()
        
    def setup_directories(self):
        """Create batch storage directories"""
        try:
            # Create directories jika belum ada
            os.makedirs(self.raw_batch_path, exist_ok=True)
            os.makedirs(self.processed_batch_path, exist_ok=True)
            os.makedirs(f"{self.batch_storage_path}/aggregated", exist_ok=True)
            
            logger.info(f"✅ Batch directories created:")
            logger.info(f"   📁 Raw batches: {self.raw_batch_path}")
            logger.info(f"   📁 Processed batches: {self.processed_batch_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to create directories: {e}")
        
    def load_real_datasets(self):
        """Load real datasets dengan integrated data"""
        try:
            logger.info("📊 Loading integrated datasets...")
            
            # Try to load processed integrated dataset first
            integrated_path = "/app/data/processed/tracks_integrated.csv"
            tracks_path = "/app/data/raw/spotify_tracks.csv"
            
            if os.path.exists(integrated_path):
                # Use pre-processed integrated data
                self.tracks_df = pd.read_csv(integrated_path)
                logger.info(f"✅ Loaded integrated dataset: {len(self.tracks_df)} tracks")
            elif os.path.exists(tracks_path):
                # Create integrated dataset on-the-fly
                from data_processor import SpotifyDataProcessor
                processor = SpotifyDataProcessor()
                self.tracks_df = processor.load_all_datasets()
                
                if self.tracks_df is not None:
                    logger.info(f"✅ Created integrated dataset: {len(self.tracks_df)} tracks")
                else:
                    logger.warning("⚠️ Failed to create integrated dataset, using fallback")
                    self.create_fallback_data()
                    return
            else:
                logger.warning("⚠️ No datasets found, using fallback")
                self.create_fallback_data()
                return
            
            # Create track features mapping dari integrated data
            for _, track in self.tracks_df.iterrows():
                track_features = self.extract_integrated_track_features(track)
                self.track_features[track['id']] = track_features
            
            logger.info(f"✅ Created UI-ready features for {len(self.track_features)} tracks")
            self.generate_user_base()
            
        except Exception as e:
            logger.error(f"❌ Failed to load datasets: {e}")
            self.create_fallback_data()

    def extract_integrated_track_features(self, track):
        """Extract features dari integrated track data"""
        return {
            # Core info
            'id': track['id'],
            'name': track.get('name', 'Unknown Track'),
            'artist_name': track.get('artist_name', 'Unknown Artist'),
            'album_name': track.get('album_name', 'Unknown Album'),
            'popularity': track.get('popularity', 50),
            'duration_ms': track.get('duration_ms', 180000),
            
            # UI elements (already processed)
            'image_url': track.get('image_url', 'https://picsum.photos/300/300?random=1'),
            'external_url': track.get('external_url', f"https://open.spotify.com/track/{track['id']}"),
            'emotion': track.get('emotion', 'neutral'),  # Real emotion dari JSON atau calculated
            'duration_display': track.get('duration_display', '3:00'),
            
            # Audio features
            'audio_features': {
                'acousticness': track.get('acousticness', 0.5),
                'danceability': track.get('danceability', 0.5),
                'energy': track.get('energy', 0.5),
                'valence': track.get('valence', 0.5),
                'tempo': track.get('tempo', 120),
                'loudness': track.get('loudness', -10),
                'speechiness': track.get('speechiness', 0.1),
                'instrumentalness': track.get('instrumentalness', 0.1),
                'liveness': track.get('liveness', 0.1),
                'key': track.get('key', 0),
                'mode': track.get('mode', 1)
            }
        }
    
    def calculate_emotion(self, valence, energy, danceability):
        """Calculate emotion berdasarkan audio features"""
        # Emotion mapping berdasarkan research
        if valence > 0.6 and energy > 0.6:
            emotions = ['Happy', 'Energetic', 'Uplifting', 'Joyful']
        elif valence > 0.6 and energy < 0.4:
            emotions = ['Peaceful', 'Calm', 'Relaxing', 'Serene']
        elif valence < 0.4 and energy > 0.6:
            emotions = ['Angry', 'Intense', 'Aggressive', 'Powerful']
        elif valence < 0.4 and energy < 0.4:
            emotions = ['Sad', 'Melancholic', 'Moody', 'Contemplative']
        else:
            emotions = ['Neutral', 'Balanced', 'Moderate']
        
        # Add danceability influence
        if danceability > 0.7:
            emotions.extend(['Danceable', 'Groovy'])
        
        return random.choice(emotions)
    
    def generate_album_image_url(self, track_name):
        """Generate realistic album image URL"""
        # For demo purposes, use placeholder with track-specific hash
        track_hash = abs(hash(track_name)) % 1000
        return f"https://picsum.photos/300/300?random={track_hash}"
    
    def create_fallback_data(self):
        """Create realistic fallback data dengan UI elements"""
        logger.info("📝 Creating UI-ready fallback track data...")
        
        fallback_tracks = [
            {
                'id': '4uLU6hMCjMI75M1A2tKUQC',
                'name': 'Shape of You',
                'artist_name': 'Ed Sheeran',
                'popularity': 95,
                'duration_ms': 233713,
                'image_url': 'https://picsum.photos/300/300?random=1',
                'external_url': 'https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC',
                'emotion': 'Happy',
                'audio_features': {
                    'acousticness': 0.581, 'danceability': 0.825, 'energy': 0.652,
                    'valence': 0.931, 'tempo': 95.977, 'loudness': -3.183,
                    'speechiness': 0.0802, 'instrumentalness': 0.0
                }
            },
            {
                'id': '7qiZfU4dY1lWllzX7mPBI3',
                'name': 'Blinding Lights',
                'artist_name': 'The Weeknd',
                'popularity': 92,
                'duration_ms': 200040,
                'image_url': 'https://picsum.photos/300/300?random=2',
                'external_url': 'https://open.spotify.com/track/7qiZfU4dY1lWllzX7mPBI3',
                'emotion': 'Energetic',
                'audio_features': {
                    'acousticness': 0.00242, 'danceability': 0.514, 'energy': 0.730,
                    'valence': 0.334, 'tempo': 171.005, 'loudness': -5.934,
                    'speechiness': 0.0598, 'instrumentalness': 0.000002
                }
            },
            {
                'id': '1mea3bSkSGXuIRvnydlB5b',
                'name': 'Levitating',
                'artist_name': 'Dua Lipa',
                'popularity': 88,
                'duration_ms': 203064,
                'image_url': 'https://picsum.photos/300/300?random=3',
                'external_url': 'https://open.spotify.com/track/1mea3bSkSGXuIRvnydlB5b',
                'emotion': 'Danceable',
                'audio_features': {
                    'acousticness': 0.012, 'danceability': 0.702, 'energy': 0.815,
                    'valence': 0.548, 'tempo': 103.0, 'loudness': -3.787,
                    'speechiness': 0.034, 'instrumentalness': 0.0
                }
            },
            {
                'id': '4iV5W9uYEdYUVa79Axb7Rh',
                'name': 'Watermelon Sugar',
                'artist_name': 'Harry Styles',
                'popularity': 85,
                'duration_ms': 174000,
                'image_url': 'https://picsum.photos/300/300?random=4',
                'external_url': 'https://open.spotify.com/track/4iV5W9uYEdYUVa79Axb7Rh',
                'emotion': 'Joyful',
                'audio_features': {
                    'acousticness': 0.122, 'danceability': 0.548, 'energy': 0.816,
                    'valence': 0.557, 'tempo': 95.0, 'loudness': -4.209,
                    'speechiness': 0.034, 'instrumentalness': 0.0
                }
            }
        ]
        
        for track in fallback_tracks:
            self.track_features[track['id']] = track
    
    def generate_user_base(self):
        """Generate realistic user base"""
        try:
            # Use smaller user base untuk demo
            self.users_sample = list(range(1, 500))  # 500 demo users
            logger.info(f"✅ Generated {len(self.users_sample)} demo users")
        except Exception as e:
            logger.warning(f"⚠️ Using default user range: {e}")
            self.users_sample = list(range(1, 100))
    
    def generate_realistic_interaction(self):
        """Generate realistic interaction dengan UI-ready data"""
        # Select random user dan track
        user_id = random.choice(self.users_sample)
        track_id = random.choice(list(self.track_features.keys()))
        track_info = self.track_features[track_id]
        
        # Generate realistic rating
        popularity = track_info.get('popularity', 50)
        base_rating = (popularity / 100) * 4 + 1
        rating_variance = random.uniform(-0.5, 0.5)
        rating = max(1, min(5, base_rating + rating_variance))
        
        # Generate action berdasarkan rating
        if rating >= 4.5:
            action_weights = {'play': 0.4, 'like': 0.3, 'replay': 0.2, 'share': 0.1}
        elif rating >= 3.5:
            action_weights = {'play': 0.5, 'like': 0.2, 'replay': 0.2, 'skip': 0.1}
        else:
            action_weights = {'play': 0.3, 'skip': 0.4, 'like': 0.1, 'replay': 0.2}
        
        action = random.choices(
            list(action_weights.keys()),
            weights=list(action_weights.values())
        )[0]
        
        # Duration berdasarkan action
        duration_ms = track_info.get('duration_ms', 180000)
        if action == 'skip':
            play_duration = random.randint(5000, 30000)  # 5-30 seconds
        elif action == 'replay':
            play_duration = duration_ms + random.randint(30000, duration_ms)
        else:
            play_duration = random.randint(30000, duration_ms)
        
        interaction = {
            # Core data untuk training
            'user_id': user_id,
            'track_id': track_id,
            'rating': round(rating, 2),
            'action': action,
            'play_duration_ms': play_duration,
            'timestamp': datetime.now().isoformat(),
            
            # UI display data - Everything needed untuk card display!
            'track_display': {
                'name': track_info['name'],
                'artist_name': track_info['artist_name'],
                'image_url': track_info['image_url'],
                'external_url': track_info['external_url'],
                'emotion': track_info['emotion'],
                'popularity': track_info['popularity'],
                'duration_ms': track_info.get('duration_ms', 180000)
            },
            
            # Audio features untuk ML training
            'audio_features': track_info['audio_features'],
            
            # Context untuk analytics
            'context': {
                'device': random.choice(['mobile', 'desktop', 'tablet']),
                'location': random.choice(['home', 'work', 'commute', 'gym']),
                'time_of_day': datetime.now().hour,
                'day_of_week': datetime.now().weekday()
            }
        }
        
        return interaction
    
    def add_to_batch(self, interaction):
        """Add interaction to current batch"""
        self.current_batch.append(interaction)
    
    def process_batch(self):
        """Process batch dengan UI-ready data"""
        if not self.current_batch:
            return
        
        self.batch_counter += 1
        timestamp = int(time.time())
        batch_id = f"batch_{self.batch_counter:06d}_{timestamp}"
        
        # Calculate batch analytics
        ratings = [i['rating'] for i in self.current_batch if i.get('rating')]
        emotions = [i['track_display']['emotion'] for i in self.current_batch]
        actions = [i['action'] for i in self.current_batch]
        
        batch_data = {
            'batch_id': batch_id,
            'timestamp': datetime.now().isoformat(),
            'size': len(self.current_batch),
            'interactions': self.current_batch,
            
            # UI summary data
            'ui_summary': {
                'featured_tracks': self.get_featured_tracks(),
                'emotion_distribution': {emotion: emotions.count(emotion) for emotion in set(emotions)},
                'avg_rating': round(sum(ratings) / len(ratings) if ratings else 0, 2),
                'popular_artists': self.get_popular_artists()
            },
            
            # Training summary
            'training_summary': {
                'unique_users': len(set(i['user_id'] for i in self.current_batch)),
                'unique_tracks': len(set(i['track_id'] for i in self.current_batch)),
                'action_distribution': {action: actions.count(action) for action in set(actions)},
                'has_audio_features': True,
                'has_ui_data': True,
                'ready_for_training': True,
                'ready_for_display': True
            }
        }
        
        logger.info(f"📦 Processing UI-ready batch {batch_id}")
        logger.info(f"   🎵 Featured tracks: {len(batch_data['ui_summary']['featured_tracks'])}")
        logger.info(f"   😊 Emotions: {list(batch_data['ui_summary']['emotion_distribution'].keys())}")
        
        # Save batch file
        batch_file_path = self.save_batch_to_file(batch_data)
        self.current_batch = []
        
        return batch_file_path
    
    def get_featured_tracks(self):
        """Get featured tracks dari current batch untuk UI display"""
        # Get unique tracks dengan highest ratings
        track_ratings = {}
        for interaction in self.current_batch:
            track_id = interaction['track_id']
            rating = interaction['rating']
            if track_id not in track_ratings or rating > track_ratings[track_id]['rating']:
                track_ratings[track_id] = {
                    'rating': rating,
                    'track_display': interaction['track_display']
                }
        
        # Sort by rating dan take top 5
        featured = sorted(track_ratings.values(), key=lambda x: x['rating'], reverse=True)[:5]
        return [track['track_display'] for track in featured]
    
    def get_popular_artists(self):
        """Get popular artists dari current batch"""
        artist_counts = {}
        for interaction in self.current_batch:
            artist = interaction['track_display']['artist_name']
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
        
        # Sort by count dan return top 3
        popular = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        return [{'artist': artist, 'interactions': count} for artist, count in popular]
    
    def save_batch_to_file(self, batch_data):
        """Save UI-ready batch data"""
        try:
            batch_filename = f"{batch_data['batch_id']}.json"
            batch_file_path = os.path.join(self.raw_batch_path, batch_filename)
            
            with open(batch_file_path, 'w') as f:
                json.dump(batch_data, f, indent=2)
            
            logger.info(f"💾 Saved UI-ready batch: {batch_file_path}")
            return batch_file_path
            
        except Exception as e:
            logger.error(f"❌ Failed to save batch: {e}")
            return None
    
    def batch_timer(self):
        """Timer untuk process batch setiap 1 menit"""
        while True:
            time.sleep(self.batch_interval)
            if self.current_batch:
                logger.info(f"⏰ Processing batch: {len(self.current_batch)} UI-ready interactions")
                self.process_batch()
    
    def continuous_streaming(self):
        """Generate continuous UI-ready streaming data"""
        logger.info("🔄 Starting UI-ready data streaming...")
        logger.info(f"🎵 Loaded {len(self.track_features)} tracks with full display data")
        logger.info(f"👥 Using {len(self.users_sample)} users")
        
        while True:
            # Generate UI-ready interaction
            interaction = self.generate_realistic_interaction()
            self.add_to_batch(interaction)
            
            # Log progress dengan UI info
            if len(self.current_batch) % 10 == 0 and len(self.current_batch) > 0:
                recent_tracks = [i['track_display']['name'] for i in self.current_batch[-3:]]
                logger.info(f"🎵 Batch: {len(self.current_batch)}, Recent: {recent_tracks}")
            
            # Process when full
            if len(self.current_batch) >= self.batch_size:
                logger.info(f"📦 Batch full - processing UI-ready data")
                self.process_batch()
            
            # Realistic streaming delay
            time.sleep(random.uniform(1, 4))
    
    def start_service(self):
        """Start UI-ready streaming service"""
        logger.info("🚀 Starting UI-Ready Music Streaming Service...")
        logger.info("🎨 Includes: track names, artists, images, emotions, external URLs")
        logger.info("🤖 Ready for: ML training + UI display")
        
        # Start batch timer
        batch_thread = threading.Thread(target=self.batch_timer, daemon=True)
        batch_thread.start()
        
        # Start streaming
        self.continuous_streaming()

if __name__ == "__main__":
    service = MusicStreamingService()
    service.start_service()