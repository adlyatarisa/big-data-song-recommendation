import pandas as pd
import json
import logging
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyDataProcessor:
    def __init__(self, data_path="/app/data/raw"):
        self.data_path = data_path
        self.tracks_df = None
        self.artists_df = None
        self.albums_df = None
        self.emotion_data = {}
        
    def load_all_datasets(self):
        """Load dan merge semua datasets"""
        try:
            logger.info("📊 Loading Spotify datasets...")
            
            # Load CSV files
            self.tracks_df = pd.read_csv(f"{self.data_path}/spotify_tracks.csv")
            self.artists_df = pd.read_csv(f"{self.data_path}/spotify_artists.csv")
            self.albums_df = pd.read_csv(f"{self.data_path}/spotify_albums.csv")
            
            logger.info(f"✅ Loaded tracks: {len(self.tracks_df)}")
            logger.info(f"✅ Loaded artists: {len(self.artists_df)}")
            logger.info(f"✅ Loaded albums: {len(self.albums_df)}")
            
            # Load emotion data
            self.load_emotion_data()
            
            # Create integrated dataset
            integrated_df = self.create_integrated_dataset()
            
            return integrated_df
            
        except Exception as e:
            logger.error(f"❌ Error loading datasets: {e}")
            return None
    
    def load_emotion_data(self):
        """Load emotion data dari JSON"""
        try:
            emotion_path = f"{self.data_path}/emotion_data.json"
            with open(emotion_path, 'r') as f:
                emotion_list = json.load(f)
            
            # Create emotion mapping: (artist, song) -> emotion
            for item in emotion_list:
                artist = item.get('artist', '').strip().lower()
                song = item.get('song', '').strip().lower()
                emotion = item.get('emotion', 'neutral')
                
                if artist and song:
                    key = f"{artist}|{song}"
                    self.emotion_data[key] = emotion
            
            logger.info(f"✅ Loaded emotions for {len(self.emotion_data)} songs")
            
        except Exception as e:
            logger.warning(f"⚠️ Could not load emotion data: {e}")
            self.emotion_data = {}
    
    def create_integrated_dataset(self):
        """Create integrated dataset dengan semua relationships"""
        try:
            logger.info("🔗 Creating integrated dataset...")
            
            # Start dengan tracks sebagai base
            df = self.tracks_df.copy()
            
            # Select only needed columns dari tracks
            track_columns = [
                'id', 'name', 'artists_id', 'album_id', 'popularity', 'duration_ms',
                'acousticness', 'danceability', 'energy', 'instrumentalness',
                'liveness', 'loudness', 'speechiness', 'tempo', 'valence', 'key',
                'mode', 'time_signature', 'preview_url', 'href'
            ]
            
            # Filter columns yang ada
            available_track_cols = [col for col in track_columns if col in df.columns]
            df = df[available_track_cols]
            
            # Merge dengan artists untuk get artist names
            if 'artists_id' in df.columns and 'id' in self.artists_df.columns:
                # Clean artist data
                artist_df = self.artists_df[['id', 'name']].drop_duplicates()
                artist_df = artist_df.rename(columns={'name': 'artist_name', 'id': 'artist_id'})
                
                # Merge artists
                df = df.merge(
                    artist_df, 
                    left_on='artists_id', 
                    right_on='artist_id', 
                    how='left'
                )
                
                logger.info(f"✅ Merged artist names for {len(df)} tracks")
            
            # Merge dengan albums untuk get album names (optional)
            if 'album_id' in df.columns and 'id' in self.albums_df.columns:
                album_df = self.albums_df[['id', 'name']].drop_duplicates()
                album_df = album_df.rename(columns={'name': 'album_name', 'id': 'album_id_merge'})
                
                df = df.merge(
                    album_df,
                    left_on='album_id',
                    right_on='album_id_merge',
                    how='left'
                )
                
                logger.info(f"✅ Merged album names for {len(df)} tracks")
            
            # Add emotions dengan smart matching
            df['emotion'] = df.apply(self.get_emotion_for_track, axis=1)
            
            # Calculate emotion success rate
            emotion_count = (df['emotion'] != 'neutral').sum()
            emotion_rate = (emotion_count / len(df)) * 100
            logger.info(f"✅ Added emotions: {emotion_count}/{len(df)} tracks ({emotion_rate:.1f}%)")
            
            # Add UI elements
            df = self.add_ui_elements(df)
            
            # Clean dan final selection
            final_df = self.clean_final_dataset(df)
            
            logger.info(f"🎯 Final integrated dataset: {len(final_df)} tracks")
            return final_df
            
        except Exception as e:
            logger.error(f"❌ Error creating integrated dataset: {e}")
            return None
    
    def get_emotion_for_track(self, row):
        """Get emotion untuk track dengan multiple matching strategies"""
        try:
            artist = str(row.get('artist_name', '')).strip().lower()
            track = str(row.get('name', '')).strip().lower()
            
            if not artist or not track:
                return self.calculate_emotion_from_features(row)
            
            # Strategy 1: Exact match
            exact_key = f"{artist}|{track}"
            if exact_key in self.emotion_data:
                return self.emotion_data[exact_key]
            
            # Strategy 2: Partial artist match
            for key, emotion in self.emotion_data.items():
                stored_artist, stored_song = key.split('|', 1)
                if artist in stored_artist or stored_artist in artist:
                    if track in stored_song or stored_song in track:
                        return emotion
            
            # Strategy 3: Calculate dari audio features
            return self.calculate_emotion_from_features(row)
            
        except Exception as e:
            return self.calculate_emotion_from_features(row)
    
    def calculate_emotion_from_features(self, row):
        """Calculate emotion dari audio features sebagai fallback"""
        try:
            valence = float(row.get('valence', 0.5))
            energy = float(row.get('energy', 0.5))
            danceability = float(row.get('danceability', 0.5))
            
            # Emotion mapping berdasarkan audio features
            if valence > 0.7 and energy > 0.7:
                return 'joy'
            elif valence > 0.6 and danceability > 0.7:
                return 'excitement'
            elif valence < 0.3 and energy < 0.4:
                return 'sadness'
            elif energy > 0.8 and valence < 0.4:
                return 'anger'
            elif valence > 0.6 and energy < 0.5:
                return 'calm'
            else:
                return 'neutral'
                
        except Exception as e:
            return 'neutral'
    
    def add_ui_elements(self, df):
        """Add UI elements untuk display"""
        try:
            # Generate image URLs
            df['image_url'] = df.apply(
                lambda row: f"https://picsum.photos/300/300?random={abs(hash(str(row['id']))) % 1000}",
                axis=1
            )
            
            # Generate external URLs
            df['external_url'] = df['id'].apply(
                lambda track_id: f"https://open.spotify.com/track/{track_id}"
            )
            
            # Add display duration (convert ms to min:sec)
            df['duration_display'] = df['duration_ms'].apply(
                lambda ms: f"{int(ms//60000)}:{int((ms%60000)//1000):02d}" if pd.notna(ms) else "0:00"
            )
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error adding UI elements: {e}")
            return df
    
    def clean_final_dataset(self, df):
        """Clean dan select final columns"""
        try:
            # Final columns untuk UI dan training
            final_columns = [
                # Core identifiers
                'id', 'name', 'artist_name', 'album_name', 'popularity',
                
                # UI elements
                'image_url', 'external_url', 'emotion', 'duration_display',
                
                # Audio features untuk ML
                'acousticness', 'danceability', 'energy', 'instrumentalness',
                'liveness', 'loudness', 'speechiness', 'tempo', 'valence',
                'key', 'mode', 'time_signature',
                
                # Additional info
                'duration_ms', 'preview_url'
            ]
            
            # Select available columns
            available_columns = [col for col in final_columns if col in df.columns]
            final_df = df[available_columns].copy()
            
            # Fill missing values
            final_df['artist_name'] = final_df['artist_name'].fillna('Unknown Artist')
            final_df['album_name'] = final_df['album_name'].fillna('Unknown Album')
            final_df['emotion'] = final_df['emotion'].fillna('neutral')
            
            # Remove duplicates
            final_df = final_df.drop_duplicates(subset=['id'])
            
            # Filter out invalid tracks
            final_df = final_df.dropna(subset=['id', 'name'])
            
            logger.info(f"🧹 Cleaned dataset: {len(final_df)} valid tracks")
            
            return final_df
            
        except Exception as e:
            logger.error(f"❌ Error cleaning dataset: {e}")
            return df

def create_processed_tracks_dataset():
    """Main function untuk create processed tracks dataset"""
    processor = SpotifyDataProcessor()
    integrated_df = processor.load_all_datasets()
    
    if integrated_df is not None:
        # Save processed dataset
        output_path = "/app/data/processed/tracks_integrated.csv"
        integrated_df.to_csv(output_path, index=False)
        logger.info(f"💾 Saved integrated dataset: {output_path}")
        
        # Print summary
        print("\n📊 Dataset Integration Summary:")
        print(f"Total tracks: {len(integrated_df)}")
        print(f"Unique artists: {integrated_df['artist_name'].nunique()}")
        print(f"Tracks with emotions: {(integrated_df['emotion'] != 'neutral').sum()}")
        print(f"Emotion distribution:")
        print(integrated_df['emotion'].value_counts().head(10))
        
        return integrated_df
    else:
        logger.error("❌ Failed to create integrated dataset")
        return None

if __name__ == "__main__":
    create_processed_tracks_dataset()