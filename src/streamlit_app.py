import streamlit as st
import requests
import pandas as pd
import os
import json
import hashlib
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="🎵 MusicBot - Personal Music Discovery",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS untuk Spotify-like appearance
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #1DB954, #1ed760);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 4px 15px rgba(29, 185, 84, 0.3);
    }
    .song-card {
        background: linear-gradient(135deg, #f8f9fa, #e9ecef);
        padding: 1.5rem;
        border-radius: 12px;
        border-left: 5px solid #1DB954;
        margin: 1rem 0;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        transition: transform 0.2s;
    }
    .song-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 20px rgba(0,0,0,0.15);
    }
    .user-card {
        background: linear-gradient(135deg, #667eea, #764ba2);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin: 1rem 0;
    }
    .genre-tag {
        background: #1DB954;
        color: white;
        padding: 0.4rem 1rem;
        border-radius: 25px;
        display: inline-block;
        margin: 0.3rem;
        font-size: 0.9rem;
        font-weight: 500;
    }
    .login-container {
        background: white;
        padding: 3rem;
        border-radius: 20px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        margin: 2rem auto;
        max-width: 500px;
    }
</style>
""", unsafe_allow_html=True)

# Session state untuk user management
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_id' not in st.session_state:
    st.session_state.user_id = None
if 'username' not in st.session_state:
    st.session_state.username = ""
if 'liked_songs' not in st.session_state:
    st.session_state.liked_songs = []
if 'user_preferences' not in st.session_state:
    st.session_state.user_preferences = {}

# API Configuration
API_URL = os.getenv('API_URL', 'http://localhost:5000')

def check_api_connection():
    """Check if API is available"""
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        return response.status_code == 200, response.json() if response.status_code == 200 else None
    except Exception as e:
        return False, str(e)

def generate_user_id(username):
    """Generate unique user ID dari username"""
    return abs(hash(username)) % 10000  # Generate ID 0-9999

def save_user_preferences(user_id, preferences):
    """Save user preferences (in real app, this would go to database)"""
    try:
        user_file = f"data/users/{user_id}_preferences.json"
        os.makedirs(os.path.dirname(user_file), exist_ok=True)
        with open(user_file, 'w') as f:
            json.dump(preferences, f)
        return True
    except:
        return False

def load_user_preferences(user_id):
    """Load user preferences"""
    try:
        user_file = f"data/users/{user_id}_preferences.json"
        if os.path.exists(user_file):
            with open(user_file, 'r') as f:
                return json.load(f)
    except:
        pass
    return {"liked_songs": [], "preferred_genres": [], "created_at": str(datetime.now())}

# Check API status
api_connected, api_data = check_api_connection()

# Header
st.markdown('''
<div class="main-header">
    <h1>🎵 MusicBot</h1>
    <p>Your Personal Music Discovery Platform</p>
</div>
''', unsafe_allow_html=True)

# LOGIN SYSTEM
if not st.session_state.logged_in:
    
    # Login/Register Section
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["🔐 Login", "📝 Register"])
    
    with tab1:
        st.subheader("Welcome Back! 👋")
        
        login_username = st.text_input("Username:", placeholder="Enter your username", key="login_user")
        login_password = st.text_input("Password:", type="password", placeholder="Enter your password", key="login_pass")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🚀 Login", use_container_width=True):
                if login_username and login_password:
                    # Generate consistent user ID
                    user_id = generate_user_id(login_username)
                    
                    # Load user preferences
                    preferences = load_user_preferences(user_id)
                    
                    # Set session state
                    st.session_state.logged_in = True
                    st.session_state.username = login_username
                    st.session_state.user_id = user_id
                    st.session_state.liked_songs = preferences.get('liked_songs', [])
                    st.session_state.user_preferences = preferences
                    
                    st.success(f"✅ Welcome back, {login_username}!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("Please enter both username and password!")
        
        with col2:
            if st.button("👤 Demo Login", use_container_width=True):
                demo_username = f"demo_user_{hash(str(datetime.now())) % 1000}"
                st.session_state.logged_in = True
                st.session_state.username = demo_username
                st.session_state.user_id = generate_user_id(demo_username)
                st.session_state.liked_songs = []
                st.success(f"✅ Demo login as {demo_username}")
                st.rerun()
    
    with tab2:
        st.subheader("Join MusicBot! 🎉")
        
        reg_username = st.text_input("Choose Username:", placeholder="e.g., music_lover_2024", key="reg_user")
        reg_password = st.text_input("Create Password:", type="password", placeholder="Enter a secure password", key="reg_pass")
        reg_email = st.text_input("Email (optional):", placeholder="your@email.com", key="reg_email")
        
        # Music preferences
        st.write("**🎵 What music do you like?**")
        preferred_genres = st.multiselect(
            "Select your favorite genres:",
            ["Pop", "Rock", "Hip Hop", "Jazz", "Classical", "Electronic", "Country", "R&B", "Reggae", "Blues"]
        )
        
        if st.button("📝 Create Account", use_container_width=True):
            if reg_username and reg_password:
                user_id = generate_user_id(reg_username)
                
                # Create user preferences
                preferences = {
                    "username": reg_username,
                    "email": reg_email,
                    "preferred_genres": preferred_genres,
                    "liked_songs": [],
                    "created_at": str(datetime.now()),
                    "user_id": user_id
                }
                
                # Save preferences
                if save_user_preferences(user_id, preferences):
                    st.session_state.logged_in = True
                    st.session_state.username = reg_username
                    st.session_state.user_id = user_id
                    st.session_state.liked_songs = []
                    st.session_state.user_preferences = preferences
                    
                    st.success(f"🎉 Account created! Welcome {reg_username}!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("Error creating account. Please try again.")
            else:
                st.error("Please enter username and password!")
    
    st.markdown('</div>', unsafe_allow_html=True)

else:
    # LOGGED IN USER INTERFACE
    
    # Sidebar
    st.sidebar.markdown(f'<div class="user-card"><h3>👋 {st.session_state.username}</h3><p>User ID: {st.session_state.user_id}</p></div>', unsafe_allow_html=True)
    
    # API Status
    if api_connected:
        st.sidebar.success("✅ Music Engine Online")
        if api_data:
            st.sidebar.info(f"🎵 {api_data.get('metadata', {}).get('total_records', 'N/A')} songs available")
    else:
        st.sidebar.error("❌ Music Engine Offline")
        st.sidebar.warning("Some features may not work")
    
    # Navigation
    page = st.sidebar.selectbox("🎵 Navigate", [
        "🏠 Home",
        "🎼 Browse Songs", 
        "🎭 Genres",
        "😊 Moods",
        "❤️ My Music",
        "🎯 Discover",
        "🔄 Real-Time Batches",
        "📊 Batch History"
    ])
    
    # User stats
    st.sidebar.markdown("---")
    st.sidebar.metric("❤️ Liked Songs", len(st.session_state.liked_songs))
    st.sidebar.metric("🎭 Preferred Genres", len(st.session_state.user_preferences.get('preferred_genres', [])))
    
    # Logout
    if st.sidebar.button("🚪 Logout"):
        # Save user data before logout
        save_user_preferences(st.session_state.user_id, {
            **st.session_state.user_preferences,
            "liked_songs": st.session_state.liked_songs,
            "last_login": str(datetime.now())
        })
        
        # Clear session
        st.session_state.logged_in = False
        st.session_state.username = ""
        st.session_state.user_id = None
        st.session_state.liked_songs = []
        st.session_state.user_preferences = {}
        st.rerun()

    # PAGE CONTENT
    if page == "🏠 Home":
        st.header(f"🏠 Welcome back, {st.session_state.username}!")
        
        # User dashboard
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("🎵 Songs Liked", len(st.session_state.liked_songs))
        with col2:
            st.metric("🆔 Your User ID", st.session_state.user_id)
        with col3:
            st.metric("🎭 Preferred Genres", len(st.session_state.user_preferences.get('preferred_genres', [])))
        with col4:
            st.metric("🎯 Ready for Recs", "✅" if len(st.session_state.liked_songs) > 0 else "❌")
        
        # Quick actions
        st.subheader("🚀 Quick Actions")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🎲 Discover Random Music", use_container_width=True):
                st.info("🎵 Discovering random songs for you...")
        
        with col2:
            if st.button("🎯 Get Recommendations", use_container_width=True):
                if len(st.session_state.liked_songs) > 0:
                    st.success("🎯 Getting personalized recommendations...")
                else:
                    st.warning("Like some songs first to get recommendations!")
        
        with col3:
            if st.button("🎭 Explore Genres", use_container_width=True):
                st.info("🎭 Exploring genre-based music...")
        
        # Recent activity
        if st.session_state.liked_songs:
            st.subheader("🕒 Your Recent Likes")
            for song in st.session_state.liked_songs[-5:]:
                st.markdown(f'<div class="song-card">🎵 {song}</div>', unsafe_allow_html=True)

    elif page == "🎼 Browse Songs":
        st.header("🎼 Browse All Songs")
        
        # Get songs from API
        if api_connected:
            try:
                response = requests.get(f"{API_URL}/songs?limit=20")
                if response.status_code == 200:
                    data = response.json()
                    songs = data.get('songs', [])
                    
                    st.success(f"📊 Showing {len(songs)} songs")
                    
                    # Display songs in cards
                    for i, song in enumerate(songs):
                        with st.container():
                            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                            
                            with col1:
                                st.markdown(f"**🎵 {song.get('name', 'Unknown Song')}**")
                                st.caption(f"👤 {song.get('artist', 'Unknown Artist')}")
                                
                                # Popularity bar
                                popularity = song.get('popularity', 0)
                                st.progress(popularity / 100)
                                st.caption(f"Popularity: {popularity}%")
                            
                            with col2:
                                song_name = song.get('name', 'Unknown')
                                if song_name not in st.session_state.liked_songs:
                                    if st.button("❤️ Like", key=f"like_{i}"):
                                        st.session_state.liked_songs.append(song_name)
                                        st.success("❤️ Added to your likes!")
                                        st.rerun()
                                else:
                                    st.button("💚 Liked", key=f"liked_{i}", disabled=True)
                            
                            with col3:
                                spotify_url = song.get('spotify_url', f"https://open.spotify.com/search/{song.get('name', '')}")
                                if st.button("🎧 Spotify", key=f"spotify_{i}"):
                                    # Direct redirect using JavaScript
                                    st.markdown(f"""
                                    <script>
                                    window.open('{spotify_url}', '_blank');
                                    </script>
                                    """, unsafe_allow_html=True)
                                    
                                    # Alternative: Show link immediately
                                    st.success(f"🎵 Opening: {song.get('name', 'Unknown')}")
                                    st.markdown(f"**[🎧 Click here if not opened automatically]({spotify_url})**")
                            
                            with col4:
                                st.markdown("---")
                else:
                    st.error("Failed to fetch songs from API")
            except Exception as e:
                st.error(f"Error: {str(e)}")
        else:
            st.warning("🔌 API not connected. Showing sample data.")
            # Sample data fallback
            sample_songs = [
                {"name": "Shape of You", "artist": "Ed Sheeran", "popularity": 95},
                {"name": "Blinding Lights", "artist": "The Weeknd", "popularity": 92},
                {"name": "Levitating", "artist": "Dua Lipa", "popularity": 88}
            ]
            
            for song in sample_songs:
                st.markdown(f'<div class="song-card"><strong>{song["name"]}</strong><br>by {song["artist"]}</div>', unsafe_allow_html=True)

    elif page == "❤️ My Music":
        st.header("❤️ Your Personal Music Library")
        
        if st.session_state.liked_songs:
            st.success(f"🎵 You have {len(st.session_state.liked_songs)} liked songs!")
            
            # Get recommendations based on liked songs
            if len(st.session_state.liked_songs) >= 3:
                st.subheader("🎯 Recommendations Based on Your Taste")
                if st.button("🔮 Get Smart Recommendations"):
                    if api_connected:
                        try:
                            # Use user's actual ID for collaborative filtering
                            response = requests.get(f"{API_URL}/recommend/collaborative/{st.session_state.user_id}?num=5")
                            if response.status_code == 200:
                                recs = response.json()
                                st.success("🎯 Here are songs you might like:")
                                for rec in recs.get('recommendations', []):
                                    st.markdown(f'<div class="song-card">🎯 Track Index: {rec.get("track_index")} - Score: {rec.get("predicted_rating", 0):.2f}</div>', unsafe_allow_html=True)
                            else:
                                st.error("Failed to get recommendations")
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
                    else:
                        st.warning("API not available for recommendations")
            
            # Show liked songs
            st.subheader("🎵 Your Liked Songs")
            for i, song in enumerate(st.session_state.liked_songs, 1):
                col1, col2, col3 = st.columns([1, 4, 1])
                with col1:
                    st.write(f"**{i}**")
                with col2:
                    st.markdown(f'<div class="song-card">🎵 {song}</div>', unsafe_allow_html=True)
                with col3:
                    if st.button("❌", key=f"remove_{i}"):
                        st.session_state.liked_songs.remove(song)
                        st.rerun()
        else:
            st.info("🎵 No liked songs yet! Start exploring to build your personal library.")

    elif page == "🎯 Discover":
        st.header("🎯 Discover New Music")
        st.write(f"**🆔 Getting recommendations for User ID: {st.session_state.user_id}**")
        
        # Show model info
        if api_connected:
            try:
                model_response = requests.get(f"{API_URL}/models/info")
                if model_response.status_code == 200:
                    model_info = model_response.json()
                    
                    st.info(f"""
                    **🤖 ML Models Loaded:**
                    - ✅ ALS Collaborative Filtering (RMSE: {model_info['model_performance']['als_rmse']:.4f})
                    - ✅ KMeans Content-Based
                    - 📊 Trained on {model_info['model_performance']['trained_records']:,} user interactions
                    """)
            except:
                pass
        
        discovery_type = st.radio("Choose discovery method:", [
            "👥 Collaborative Filtering (ALS Model)",
            "🎵 Content-Based (KMeans Model)", 
            "🎲 Random Discovery",
            "🆕 Popular Songs"
        ])
        
        if st.button("🚀 Discover Music!", use_container_width=True):
            
            with st.spinner('🔍 Using ML models to discover music...'):
                
                if discovery_type == "👥 Collaborative Filtering (ALS Model)" and api_connected:
                    try:
                        response = requests.get(f"{API_URL}/recommend/collaborative/{st.session_state.user_id}?num=10")
                        if response.status_code == 200:
                            data = response.json()
                            recommendations = data.get('recommendations', [])
                            
                            if recommendations:
                                st.success(f"🎯 ALS Model found {len(recommendations)} personalized recommendations!")
                                st.info(f"📊 Based on {data.get('trained_on', 'N/A')} user interactions")
                                
                                # Display real model recommendations
                                for i, rec in enumerate(recommendations, 1):
                                    with st.container():
                                        col1, col2, col3 = st.columns([1, 4, 2])
                                        
                                        with col1:
                                            st.markdown(f"**#{i}**")
                                        
                                        with col2:
                                            st.markdown(f"""
                                            <div class="song-card">
                                                🎯 <strong>{rec.get('track_name', 'Unknown')}</strong><br>
                                                👤 by {rec.get('artist_name', 'Unknown Artist')}<br>
                                                ⭐ Predicted Rating: {rec.get('predicted_rating', 0):.2f}/5.0<br>
                                                🔢 Track Index: {rec.get('track_index', 'N/A')}
                                            </div>
                                            """, unsafe_allow_html=True)
                                        
                                        with col3:
                                            if st.button("❤️ Like", key=f"als_like_{i}"):
                                                track_name = rec.get('track_name', f"Track_{rec.get('track_index')}")
                                                if track_name not in st.session_state.liked_songs:
                                                    st.session_state.liked_songs.append(track_name)
                                                    st.success("❤️ Added to liked songs!")
                                                
                                                spotify_url = rec.get('spotify_url', '#')
                                                if st.button("🎧", key=f"als_spotify_{i}"):
                                                    st.markdown(f"[🎵 Open Spotify]({spotify_url})")
                        else:
                            st.warning("No recommendations found. Try different user ID!")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                
                elif discovery_type == "🎵 Content-Based (KMeans Model)" and api_connected:
                    # Content-based recommendations
                    if st.session_state.liked_songs:
                        seed_song = st.session_state.liked_songs[-1]  # Use last liked song
                        try:
                            response = requests.get(f"{API_URL}/recommend/content/{seed_song}?num=5")
                            if response.status_code == 200:
                                data = response.json()
                                recommendations = data.get('recommendations', [])
                                
                                st.success(f"🎵 KMeans found {len(recommendations)} similar to '{seed_song}'!")
                                
                                for rec in recommendations:
                                    st.markdown(f"""
                                    <div class="song-card">
                                        🎵 <strong>{rec.get('name', 'Unknown')}</strong><br>
                                        👤 by {rec.get('artist', 'Unknown')}<br>
                                        📊 Similarity: {rec.get('similarity_score', 0):.2f}
                                    </div>
                                    """, unsafe_allow_html=True)
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
                    else:
                        st.warning("Like some songs first for content-based recommendations!")
                
                else:
                    st.success(f"🎵 Discovering music using: {discovery_type}")
                    st.info("💡 This feature will be enhanced with more discovery algorithms!")
    
    elif page == "🔄 Real-Time Batches":
        """Show real-time batch monitoring dashboard"""
        st.title("🔄 Real-Time Batch Monitoring")
        
        # Auto-refresh every 30 seconds
        if st.button("🔄 Refresh Data"):
            st.rerun()
        
        try:
            # Get latest batch data
            response = requests.get(f"{API_URL}/batches/latest", timeout=10)
            if response.status_code == 200:
                batch_data = response.json()
                latest_batch = batch_data['latest_batch']
                
                # Show batch info
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Batch ID", latest_batch['batch_id'])
                with col2:
                    st.metric("Interactions", latest_batch['size'])
                with col3:
                    st.metric("Unique Users", latest_batch['training_summary']['unique_users'])
                with col4:
                    st.metric("Avg Rating", f"{latest_batch['ui_summary']['avg_rating']:.2f}")
                
                # Featured Tracks Cards
                st.subheader("🎵 Featured Tracks dari Latest Batch")
                
                featured_tracks = latest_batch['ui_summary']['featured_tracks']
                
                # Create columns for track cards
                cols = st.columns(min(len(featured_tracks), 4))
                
                for i, track in enumerate(featured_tracks[:4]):
                    with cols[i]:
                        # Create track card
                        st.image(track['image_url'], width=200)
                        st.markdown(f"**{track['name']}**")
                        st.markdown(f"*{track['artist_name']}*")
                        st.markdown(f"😊 {track['emotion']}")
                        st.markdown(f"⭐ {track['popularity']}/100")
                        
                        # Clickable link to Spotify
                        st.markdown(f"[🎵 Play on Spotify]({track['external_url']})")
                
                # Emotion Distribution Chart
                st.subheader("😊 Emotion Distribution")
                
                emotion_data = latest_batch['ui_summary']['emotion_distribution']
                if emotion_data:
                    # Create pie chart
                    fig = px.pie(
                        values=list(emotion_data.values()),
                        names=list(emotion_data.keys()),
                        title="Distribution of Track Emotions",
                        color_discrete_sequence=px.colors.qualitative.Set3
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Action Distribution
                st.subheader("🎯 User Actions")
                
                action_data = latest_batch['training_summary']['action_distribution']
                if action_data:
                    fig = px.bar(
                        x=list(action_data.keys()),
                        y=list(action_data.values()),
                        title="User Action Distribution",
                        color=list(action_data.values()),
                        color_continuous_scale="viridis"
                    )
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                
                # Popular Artists
                st.subheader("🎤 Popular Artists")
                
                popular_artists = latest_batch['ui_summary']['popular_artists']
                if popular_artists:
                    artist_df = pd.DataFrame(popular_artists)
                    
                    fig = px.bar(
                        artist_df,
                        x='artist',
                        y='interactions',
                        title="Most Popular Artists in Latest Batch",
                        color='interactions',
                        color_continuous_scale="blues"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Raw batch data (expandable)
                with st.expander("🔍 View Raw Batch Data"):
                    st.json(latest_batch)
                    
            else:
                st.error("❌ Could not fetch batch data")
                
        except Exception as e:
            st.error(f"❌ Error: {e}")

    elif page == "📊 Batch History":
        st.title("📊 Batch History & Trends")
        
        try:
            # Get all batch files
            response = requests.get(f"{API_URL}/batches/files", timeout=10)
            if response.status_code == 200:
                data = response.json()
                batch_files = data['batch_files']
                
                if batch_files:
                    # Convert to DataFrame untuk analysis
                    df = pd.DataFrame(batch_files)
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    
                    # Batch size over time
                    st.subheader("📈 Batch Size Trends")
                    fig = px.line(
                        df,
                        x='timestamp',
                        y='size',
                        title="Batch Size Over Time",
                        markers=True
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Summary statistics
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Total Batches", len(batch_files))
                    with col2:
                        st.metric("Avg Batch Size", f"{df['size'].mean():.1f}")
                    with col3:
                        st.metric("Total Interactions", df['size'].sum())
                    
                    # Batch files table
                    st.subheader("📋 All Batch Files")
                    display_df = df[['batch_id', 'size', 'timestamp']].copy()
                    display_df['timestamp'] = display_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    st.dataframe(display_df, use_container_width=True)
                else:
                    st.info("📝 No batch files found yet")
                    
        except Exception as e:
            st.error(f"❌ Error: {e}")