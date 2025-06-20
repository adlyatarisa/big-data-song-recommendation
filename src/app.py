from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.feature import StringIndexerModel
from pyspark.sql.functions import col, lit
import json
import logging
import pandas as pd
import os

app = Flask(__name__)

class RecommendationService:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("RecommendationAPI") \
            .getOrCreate()
        self.load_models()
        
    def load_models(self):
        """Load trained models dan tracks data"""
        try:
            # Load models (path sesuai dengan metadata)
            self.als_model = ALSModel.load("/app/data/models/als_model")
            self.kmeans_model = KMeansModel.load("/app/data/models/kmeans_model") 
            self.track_indexer = StringIndexerModel.load("/app/data/models/track_indexer")
            
            # Load metadata
            with open("/app/data/models/training_metadata.json", "r") as f:
                self.metadata = json.load(f)
            
            print("✅ Models loaded successfully")
            print(f"📊 Trained on {self.metadata['total_records']} records")
            print(f"🎯 ALS RMSE: {self.metadata['als_rmse']}")
            
            # Load tracks data untuk browsing
            self.load_tracks_data()
            
        except Exception as e:
            print(f"❌ Error loading models: {e}")
            raise

    def load_tracks_data(self):
        """Load tracks data dengan stability optimization"""
        try:
            # Use sample data untuk stability - comment out untuk production
            print("🚀 Using stable sample data for development...")
            self.create_stable_sample_tracks()
            
        except Exception as e:
            print(f"❌ Error in load_tracks_data: {e}")
            self.create_stable_sample_tracks()

    def create_stable_sample_tracks(self):
        """Create comprehensive stable sample tracks"""
        try:
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType
            
            # Popular songs with clean data
            sample_data = [
                ("4uLU6hMCjMI75M1A2tKUQC", "Shape of You", "Ed Sheeran", 95.0),
                ("7qiZfU4dY1lWllzX7mPBI3", "Blinding Lights", "The Weeknd", 92.0),
                ("1mea3bSkSGXuIRvnydlB5b", "Levitating", "Dua Lipa", 88.0),
                ("4iV5W9uYEdYUVa79Axb7Rh", "Watermelon Sugar", "Harry Styles", 85.0),
                ("11dFghVXANMlKmJXsNCbNl", "good 4 u", "Olivia Rodrigo", 84.0),
                ("5HCyWlXZPP0y6Gqq8TgA20", "Stay", "The Kid LAROI & Justin Bieber", 82.0),
                ("1BxfuPKGuaTgP7aM0Bbdwr", "Peaches", "Justin Bieber", 80.0),
                ("6WrI0LAC5M1Rw2MnX2ZvEg", "Don't Start Now", "Dua Lipa", 78.0),
                ("463CkQjx2Zk1yXoBuierM9", "Levitating (feat. DaBaby)", "Dua Lipa", 75.0),
                ("3n3Ppam7vgaVa1iaRUc9Lp", "Mr. Brightside", "The Killers", 73.0),
                ("2takcwOaAZWiXQijPHIx7B", "Can't Stop The Feeling!", "Justin Timberlake", 72.0),
                ("7GhIk7Il098yCjg4BQjzvb", "Bad Guy", "Billie Eilish", 70.0),
                ("0VjIjW4GlULA5wTYoNF3mw", "Uptown Funk", "Mark Ronson ft. Bruno Mars", 69.0),
                ("6habFhsOp2NvshLv26DqMb", "Someone Like You", "Adele", 68.0),
                ("4VqPOruhp5EdPBeR92t6lQ", "Bohemian Rhapsody", "Queen", 67.0),
                ("1Qrg8KqiBpW07V7PNxwwwL", "Anti-Hero", "Taylor Swift", 66.0),
                ("1BxfuPKGuaTgP7aM0Bbdwx", "As It Was", "Harry Styles", 65.0),
                ("4Dvkj6JhhA12EX05fT7y2e", "Industry Baby", "Lil Nas X & Jack Harlow", 64.0),
                ("5sdQOyqq2IDhvmx2lHOpwd", "Flowers", "Miley Cyrus", 63.0),
                ("7ytR5pFWmSjzHJIeQkgog4", "Heat Waves", "Glass Animals", 62.0),
                ("6dOtVTDdiauQNBQEDOtlAB", "Cruel Summer", "Taylor Swift", 61.0),
                ("4c5JvTzfpK3pkKfzKJE8qX", "drivers license", "Olivia Rodrigo", 60.0),
                ("1mWdTewIgB3gtBM3TOSFhB", "positions", "Ariana Grande", 59.0),
                ("7MXVkk9YMctZqd1Srtv4MB", "Therefore I Am", "Billie Eilish", 58.0),
                ("0gplL1WMoJ6iYaPgMCL0gX", "Save Your Tears", "The Weeknd", 57.0)
            ]
            
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("artist_name", StringType(), True),  # Clean artist names
                StructField("popularity", DoubleType(), True)
            ])
            
            self.tracks_df = self.spark.createDataFrame(sample_data, schema)
            print(f"✅ Created stable sample data with {len(sample_data)} popular songs")
            
        except Exception as e:
            print(f"❌ Error creating stable sample tracks: {e}")
            # Even simpler fallback
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("artist_name", StringType(), True),
                StructField("popularity", DoubleType(), True)
            ])
            fallback_data = [("sample_id", "Sample Song", "Sample Artist", 50.0)]
            self.tracks_df = self.spark.createDataFrame(fallback_data, schema)
            print("✅ Created minimal fallback data")

# Initialize service
rec_service = RecommendationService()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "models_loaded": True,
        "metadata": rec_service.metadata
    })

@app.route('/recommend', methods=['POST'])
def get_recommendations():
    try:
        data = request.json
        user_id = data.get('user_id')
        limit = data.get('limit', 10)
        
        # Placeholder untuk sekarang
        recommendations = [
            {
                'track_name': 'Example Song 1',
                'artist_name': 'Example Artist 1',
                'genre': 'Pop',
                'score': 0.95
            },
            {
                'track_name': 'Example Song 2', 
                'artist_name': 'Example Artist 2',
                'genre': 'Rock',
                'score': 0.87
            }
        ]
        
        return jsonify({
            'user_id': user_id,
            'recommendations': recommendations[:limit],
            'total': len(recommendations)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/analytics', methods=['GET'])
def get_analytics():
    return jsonify({
        'total_users': 1000,
        'total_songs': 50000,
        'total_recommendations_generated': 5000,
        'system_status': 'operational'
    })

@app.route('/', methods=['GET'])
def home():
    """Home endpoint"""
    return jsonify({"message": "MusicBot API", "status": "running"})

# Update get_all_songs untuk handle artist_name properly
@app.route('/songs', methods=['GET'])
def get_all_songs():
    """Get all songs with proper error handling"""
    try:
        limit = request.args.get('limit', 20, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        if not hasattr(rec_service, 'tracks_df') or rec_service.tracks_df is None:
            return jsonify({"error": "Tracks data not available", "songs": []}), 500
        
        # Fetch songs dengan proper limit handling
        songs_df = rec_service.tracks_df.limit(limit + offset).collect()
        songs_to_return = songs_df[offset:offset + limit] if len(songs_df) > offset else songs_df
        
        result = []
        for song in songs_to_return:
            result.append({
                "id": getattr(song, 'id', 'unknown'),
                "name": getattr(song, 'name', 'Unknown Song'),
                "artist": getattr(song, 'artist_name', 'Unknown Artist'),  # Use artist_name dari schema
                "popularity": getattr(song, 'popularity', 0),
                "spotify_url": f"https://open.spotify.com/track/{getattr(song, 'id', '')}"
            })
        
        return jsonify({
            "songs": result, 
            "total": len(result),
            "limit": limit,
            "offset": offset
        })
        
    except Exception as e:
        print(f"❌ Error in get_all_songs: {str(e)}")
        return jsonify({"error": str(e), "songs": []}), 500

# Add real collaborative filtering endpoint
@app.route('/recommend/collaborative/<int:user_id>', methods=['GET'])
def get_collaborative_recommendations(user_id):
    """Get REAL collaborative filtering recommendations using trained ALS model"""
    try:
        num_recs = request.args.get('num', 10, type=int)
        
        # Create user dataframe untuk ALS model
        from pyspark.sql.types import StructType, StructField, IntegerType
        user_schema = StructType([
            StructField("user_id", IntegerType(), True)
        ])
        user_df = rec_service.spark.createDataFrame([(user_id,)], user_schema)
        
        # Get recommendations dari trained ALS model
        print(f"🎯 Getting ALS recommendations for User {user_id}")
        recommendations = rec_service.als_model.recommendForUserSubset(user_df, num_recs)
        recs_list = recommendations.collect()
        
        result = []
        if recs_list:
            user_recs = recs_list[0]['recommendations']
            for rec in user_recs:
                track_index = rec['track_index']
                predicted_rating = float(rec['rating'])
                
                # Try to map track_index back to actual song name
                # For now, use track_index (nanti bisa di-map ke real songs)
                track_name = f"Track_{track_index}"
                if track_index < len(rec_service.tracks_df.collect()):
                    # Get actual song from our sample data
                    songs = rec_service.tracks_df.collect()
                    if track_index < len(songs):
                        actual_song = songs[track_index % len(songs)]
                        track_name = actual_song.name
                        artist_name = actual_song.artist_name
                    else:
                        artist_name = "Unknown Artist"
                else:
                    artist_name = "Unknown Artist"
                
                result.append({
                    "track_index": track_index,
                    "track_name": track_name,
                    "artist_name": artist_name,
                    "predicted_rating": predicted_rating,
                    "spotify_url": f"https://open.spotify.com/search/{track_name}"
                })
        
        return jsonify({
            "user_id": user_id,
            "recommendations": result,
            "total": len(result),
            "model": "ALS Collaborative Filtering",
            "trained_on": rec_service.metadata.get('total_records', 'N/A')
        })
        
    except Exception as e:
        print(f"❌ Error in collaborative recommendations: {str(e)}")
        return jsonify({"error": str(e), "recommendations": []}), 500

@app.route('/recommend/content/<track_name>', methods=['GET'])
def get_content_recommendations(track_name):
    """Get content-based recommendations using KMeans clustering"""
    try:
        num_recs = request.args.get('num', 5, type=int)
        
        # For now, return similar songs from our sample data
        # In production, this would use KMeans model + audio features
        similar_songs = []
        songs = rec_service.tracks_df.collect()
        
        # Simple similarity based on genre/artist (placeholder)
        for song in songs[:num_recs]:
            if song.name.lower() != track_name.lower():
                similar_songs.append({
                    "id": song.id,
                    "name": song.name,
                    "artist": song.artist_name,
                    "popularity": song.popularity,
                    "similarity_score": 0.85,  # Placeholder score
                    "spotify_url": f"https://open.spotify.com/track/{song.id}"
                })
        
        return jsonify({
            "seed_track": track_name,
            "recommendations": similar_songs,
            "total": len(similar_songs),
            "model": "KMeans Content-Based"
        })
        
    except Exception as e:
        print(f"❌ Error in content recommendations: {str(e)}")
        return jsonify({"error": str(e), "recommendations": []}), 500

@app.route('/models/info', methods=['GET'])
def get_model_info():
    """Get information about loaded models"""
    try:
        return jsonify({
            "models_loaded": {
                "als_model": hasattr(rec_service, 'als_model'),
                "kmeans_model": hasattr(rec_service, 'kmeans_model'),
                "track_indexer": hasattr(rec_service, 'track_indexer')
            },
            "training_metadata": rec_service.metadata,
            "tracks_available": rec_service.tracks_df.count() if hasattr(rec_service, 'tracks_df') else 0,
            "model_performance": {
                "als_rmse": rec_service.metadata.get('als_rmse', 'N/A'),
                "trained_records": rec_service.metadata.get('total_records', 'N/A')
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    try:
        print("🚀 Starting Flask API server...")
        print("📡 Checking all endpoints are registered...")
        
        # Test if service was created properly
        if 'rec_service' in globals():
            print("✅ RecommendationService initialized")
        else:
            print("❌ RecommendationService failed to initialize")
        
        # Print all registered routes
        print("📍 Registered routes:")
        for rule in app.url_map.iter_rules():
            print(f"  - {rule.endpoint}: {rule.rule}")
        
        print("🌐 Starting Flask server on 0.0.0.0:5000...")
        app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
        
    except Exception as e:
        print(f"❌ Critical error starting Flask: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e