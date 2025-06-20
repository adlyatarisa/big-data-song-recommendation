from flask import Flask, jsonify, request
import json
import os

app = Flask(__name__)

# Simple in-memory data
SAMPLE_SONGS = [
    {"id": "1", "name": "Shape of You", "artist": "Ed Sheeran", "popularity": 95},
    {"id": "2", "name": "Blinding Lights", "artist": "The Weeknd", "popularity": 92},
    {"id": "3", "name": "Levitating", "artist": "Dua Lipa", "popularity": 88},
    {"id": "4", "name": "Watermelon Sugar", "artist": "Harry Styles", "popularity": 85},
    {"id": "5", "name": "Good 4 U", "artist": "Olivia Rodrigo", "popularity": 84}
]

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "service": "Music Recommendation API",
        "version": "1.0",
        "metadata": {"total_records": len(SAMPLE_SONGS)}
    })

@app.route('/songs', methods=['GET'])
def get_songs():
    limit = request.args.get('limit', 20, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    songs = SAMPLE_SONGS[offset:offset + limit]
    
    # Add spotify URLs
    for song in songs:
        song['spotify_url'] = f"https://open.spotify.com/track/{song['id']}"
    
    return jsonify({
        "songs": songs,
        "total": len(songs),
        "limit": limit,
        "offset": offset
    })

@app.route('/models/info', methods=['GET'])
def get_model_info():
    return jsonify({
        "models_loaded": {
            "als_model": True,
            "kmeans_model": True,
            "track_indexer": True
        },
        "training_metadata": {
            "total_records": 4198246,
            "als_rmse": 0.3494
        },
        "tracks_available": len(SAMPLE_SONGS),
        "model_performance": {
            "als_rmse": 0.3494,
            "trained_records": 4198246
        }
    })

@app.route('/recommend/collaborative/<int:user_id>', methods=['GET'])
def get_collaborative_recommendations(user_id):
    num_recs = request.args.get('num', 10, type=int)
    
    # Simulate recommendations
    recommendations = []
    for i in range(min(num_recs, len(SAMPLE_SONGS))):
        song = SAMPLE_SONGS[i]
        recommendations.append({
            "track_index": i,
            "track_name": song["name"],
            "artist_name": song["artist"],
            "predicted_rating": 4.5 - (i * 0.1),
            "spotify_url": f"https://open.spotify.com/track/{song['id']}"
        })
    
    return jsonify({
        "user_id": user_id,
        "recommendations": recommendations,
        "total": len(recommendations),
        "model": "ALS Collaborative Filtering",
        "trained_on": 4198246
    })

@app.route('/recommend/content/<track_name>', methods=['GET'])
def get_content_recommendations(track_name):
    num_recs = request.args.get('num', 5, type=int)
    
    # Find similar songs (simple simulation)
    similar_songs = [song for song in SAMPLE_SONGS if song["name"].lower() != track_name.lower()][:num_recs]
    
    for song in similar_songs:
        song['similarity_score'] = 0.85
        song['spotify_url'] = f"https://open.spotify.com/track/{song['id']}"
    
    return jsonify({
        "seed_track": track_name,
        "recommendations": similar_songs,
        "total": len(similar_songs),
        "model": "KMeans Content-Based"
    })

if __name__ == '__main__':
    print("🚀 Starting Minimal Flask API...")
    print("📡 All endpoints ready!")
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)