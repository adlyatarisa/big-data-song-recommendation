from flask import Flask, request, jsonify
import json
import logging
import threading
import time
from datetime import datetime
from kafka import KafkaConsumer
from minio import Minio
import os
import pickle
import joblib
from typing import Dict, List, Optional
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

class ModelManager:
    def __init__(self, models_path="/app/data/models"):
        self.models_path = models_path
        self.config_path = os.path.join(models_path, "config")
        self.metadata_path = os.path.join(models_path, "metadata")
        self.versions_path = os.path.join(models_path, "versions")
        
        # Ensure directories exist
        for path in [self.config_path, self.metadata_path, self.versions_path]:
            os.makedirs(path, exist_ok=True)
    
    def save_model(self, model, model_type: str, version: Optional[str] = None, 
                   metadata: Optional[Dict] = None):
        """Save model dengan versioning dan metadata"""
        try:
            # Generate version if not provided
            if version is None:
                version = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save paths
            model_dir = os.path.join(self.models_path, model_type.lower())
            os.makedirs(model_dir, exist_ok=True)
            
            model_filename = f"model_{version}.pkl"
            model_path = os.path.join(model_dir, model_filename)
            
            # Save model
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
            
            # Save metadata
            if metadata:
                metadata_filename = f"metadata_{version}.json"
                metadata_path = os.path.join(self.metadata_path, metadata_filename)
                
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2)
            
            # Update best model link
            best_model_path = os.path.join(model_dir, "best_model.pkl")
            if os.path.exists(model_path):
                # Create symlink to best model
                if os.path.exists(best_model_path):
                    os.remove(best_model_path)
                os.symlink(model_filename, best_model_path)
            
            logger.info(f"✅ Model saved: {model_path}")
            return model_path
            
        except Exception as e:
            logger.error(f"❌ Failed to save model: {e}")
            return None
    
    def load_model(self, model_type: str, version: str = "best"):
        """Load model by type and version"""
        try:
            model_dir = os.path.join(self.models_path, model_type.lower())
            
            if version == "best":
                model_path = os.path.join(model_dir, "best_model.pkl")
            else:
                model_path = os.path.join(model_dir, f"model_{version}.pkl")
            
            if os.path.exists(model_path):
                with open(model_path, 'rb') as f:
                    model = pickle.load(f)
                logger.info(f"✅ Model loaded: {model_path}")
                return model
            else:
                logger.warning(f"⚠️ Model not found: {model_path}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to load model: {e}")
            return None
    
    def list_models(self, model_type: Optional[str] = None):
        """List available models"""
        try:
            models = {}
            
            if model_type:
                model_types = [model_type.lower()]
            else:
                model_types = [d for d in os.listdir(self.models_path) 
                              if os.path.isdir(os.path.join(self.models_path, d))]
            
            for mtype in model_types:
                model_dir = os.path.join(self.models_path, mtype)
                if os.path.exists(model_dir):
                    model_files = [f for f in os.listdir(model_dir) if f.endswith('.pkl')]
                    models[mtype] = sorted(model_files)
            
            return models
            
        except Exception as e:
            logger.error(f"❌ Failed to list models: {e}")
            return {}

class AutoRetrainingService:
    def __init__(self):
        self.minio_client = None
        self.kafka_consumer = None
        self.is_training = False
        self.last_training_time = None
        self.batch_queue = []
        
        self.model_manager = ModelManager()
        
        self.setup_minio()
        self.setup_kafka_consumer()
        
        # Auto-training settings
        self.auto_training_enabled = True
        self.batch_check_interval = 60  # Check every minute
        self.min_batches_for_training = 3  # Minimum batches to trigger training
    
    def setup_minio(self):
        """Setup MinIO client"""
        try:
            self.minio_client = Minio(
                'minio:9000',
                access_key='minioadmin',
                secret_key='minioadmin123',
                secure=False
            )
            logger.info("✅ Training service - MinIO connected")
        except Exception as e:
            logger.error(f"❌ MinIO connection failed: {e}")
    
    def setup_kafka_consumer(self):
        """Setup Kafka consumer untuk model updates"""
        try:
            self.kafka_consumer = KafkaConsumer(
                'processed-batches',
                'model-updates',
                bootstrap_servers=['kafka:9092'],  # ✅ Use Docker service name
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                group_id='training-service',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=30000  # ✅ Add timeout to prevent hanging
            )
            logger.info("✅ Kafka consumer initialized")
        except Exception as e:
            logger.error(f"❌ Kafka consumer setup failed: {e}")
            # Don't fail the service if Kafka is not available
            self.kafka_consumer = None
    
    def listen_for_retraining_signals(self):
        """Listen untuk retraining signals dari Kafka"""
        logger.info("🎧 Listening for retraining signals...")
        
        for message in self.kafka_consumer:
            try:
                data = message.value
                topic = message.topic
                
                if topic == 'processed-batches':
                    batch_id = data.get('batch_id')
                    batch_size = data.get('size', 0)
                    
                    logger.info(f"📦 Received batch signal: {batch_id} ({batch_size} interactions)")
                    
                    # Add to queue untuk processing
                    self.batch_queue.append({
                        'batch_id': batch_id,
                        'size': batch_size,
                        'timestamp': data.get('timestamp'),
                        'received_at': datetime.now().isoformat()
                    })
                    
                    # Trigger retraining if not already running
                    if not self.is_training and len(self.batch_queue) >= 1:
                        threading.Thread(target=self.start_retraining, daemon=True).start()
                
                elif topic == 'model-updates':
                    logger.info(f"🔄 Model update signal: {data}")
                    
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")
    
    def start_retraining(self):
        """Start model retraining process"""
        if self.is_training:
            logger.info("🔄 Training already in progress, skipping...")
            return
        
        self.is_training = True
        self.last_training_time = datetime.now()
        
        try:
            logger.info("🚀 Starting model retraining...")
            
            # Process queued batches
            batches_to_process = self.batch_queue.copy()
            self.batch_queue.clear()
            
            total_interactions = sum(batch['size'] for batch in batches_to_process)
            
            logger.info(f"📊 Retraining with {len(batches_to_process)} batches ({total_interactions} interactions)")
            
            # Simulate training process (replace dengan actual training)
            self.simulate_training(batches_to_process)
            
            # Save updated model metadata
            self.update_model_metadata(batches_to_process)
            
            logger.info("✅ Model retraining completed!")
            
        except Exception as e:
            logger.error(f"❌ Retraining failed: {e}")
        finally:
            self.is_training = False
    
    def simulate_training(self, batches):
        """Simulate model training process"""
        logger.info("🧠 Simulating ALS model retraining...")
        time.sleep(10)  # Simulate training time
        
        logger.info("🎯 Simulating KMeans model retraining...")
        time.sleep(5)  # Simulate training time
        
        # In real implementation, this would:
        # 1. Load new batch data from MinIO
        # 2. Combine with existing training data
        # 3. Retrain ALS and KMeans models
        # 4. Save updated models to /app/data/models/
        
    def update_model_metadata(self, batches):
        """Update model metadata after retraining"""
        try:
            # Load existing metadata
            metadata_path = "/app/data/models/training_metadata.json"
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
            else:
                metadata = {}
            
            # Update metadata
            total_new_interactions = sum(batch['size'] for batch in batches)
            metadata['total_records'] = metadata.get('total_records', 0) + total_new_interactions
            metadata['last_retraining'] = datetime.now().isoformat()
            metadata['batches_processed'] = metadata.get('batches_processed', 0) + len(batches)
            metadata['retraining_count'] = metadata.get('retraining_count', 0) + 1
            
            # Save updated metadata
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"📝 Updated metadata: {total_new_interactions} new interactions")
            
        except Exception as e:
            logger.error(f"❌ Failed to update metadata: {e}")
    
    def clean_old_models(self, keep_versions: int = 5):
        """Clean old model versions, keep only recent ones"""
        try:
            models = self.model_manager.list_models()
            
            for model_type, model_files in models.items():
                if len(model_files) > keep_versions:
                    # Sort by creation time, keep newest
                    model_dir = os.path.join(self.model_manager.models_path, model_type)
                    files_with_time = []
                    
                    for file in model_files:
                        if file != "best_model.pkl":  # Don't delete best model link
                            file_path = os.path.join(model_dir, file)
                            creation_time = os.path.getctime(file_path)
                            files_with_time.append((creation_time, file_path))
                    
                    # Sort by creation time (oldest first)
                    files_with_time.sort()
                    
                    # Delete oldest files
                    files_to_delete = files_with_time[:-keep_versions]
                    for _, file_path in files_to_delete:
                        os.remove(file_path)
                        logger.info(f"🗑️ Deleted old model: {file_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to clean old models: {e}")
    
    def get_pending_batches(self):
        """Get list of unprocessed batch files"""
        try:
            batch_raw_path = "/app/data/batches/raw"
            batch_processed_path = "/app/data/batches/processed"
            
            if not os.path.exists(batch_raw_path):
                logger.warning(f"⚠️ Batch raw path not found: {batch_raw_path}")
                return []
            
            # Get all raw batch files
            raw_batches = []
            for filename in os.listdir(batch_raw_path):
                if filename.endswith('.json'):
                    file_path = os.path.join(batch_raw_path, filename)
                    raw_batches.append(file_path)
            
            # Get processed batch files
            processed_files = set()
            if os.path.exists(batch_processed_path):
                for filename in os.listdir(batch_processed_path):
                    if filename.endswith('.json'):
                        processed_files.add(filename)
            
            # Filter out already processed batches
            pending_batches = []
            for batch_path in raw_batches:
                filename = os.path.basename(batch_path)
                if filename not in processed_files:
                    pending_batches.append(batch_path)
            
            logger.info(f"📊 Found {len(pending_batches)} pending batches")
            return sorted(pending_batches)
            
        except Exception as e:
            logger.error(f"❌ Error getting pending batches: {e}")
            return []
    
    def get_processed_batches(self):
        """Get list of processed batch files"""
        try:
            batch_processed_path = "/app/data/batches/processed"
            
            if not os.path.exists(batch_processed_path):
                return []
            
            processed_batches = []
            for filename in os.listdir(batch_processed_path):
                if filename.endswith('.json'):
                    file_path = os.path.join(batch_processed_path, filename)
                    processed_batches.append(file_path)
            
            return sorted(processed_batches)
            
        except Exception as e:
            logger.error(f"❌ Error getting processed batches: {e}")
            return []
    
    def auto_training_loop(self):
        """Automatic training loop - checks for new batches periodically"""
        logger.info("🔄 Starting auto-training loop...")
        
        while self.auto_training_enabled:
            try:
                if not self.is_training:
                    pending_batches = self.get_pending_batches()
                    
                    if len(pending_batches) >= self.min_batches_for_training:
                        logger.info(f"🚀 Auto-triggering training with {len(pending_batches)} batches")
                        self.process_batches_for_training()
                    else:
                        logger.info(f"⏳ Waiting for more batches ({len(pending_batches)}/{self.min_batches_for_training})")
                
                # Wait before next check
                time.sleep(self.batch_check_interval)
                
            except Exception as e:
                logger.error(f"❌ Error in auto-training loop: {e}")
                time.sleep(self.batch_check_interval)
    
    def process_batches_for_training(self):
        """Process pending batches for ML training"""
        try:
            self.is_training = True
            self.last_training = datetime.now().isoformat()
            
            logger.info("🤖 Starting batch processing for training...")
            
            # Get pending batches
            pending_batches = self.get_pending_batches()
            
            if not pending_batches:
                logger.warning("⚠️ No pending batches for training")
                return
            
            # Load and combine batch data
            all_interactions = []
            processed_batch_files = []
            
            for batch_path in pending_batches:
                try:
                    with open(batch_path, 'r') as f:
                        batch_data = json.load(f)
                    
                    # Extract interactions
                    interactions = batch_data.get('interactions', [])
                    all_interactions.extend(interactions)
                    processed_batch_files.append(batch_path)
                    
                    logger.info(f"✅ Processed batch: {os.path.basename(batch_path)} ({len(interactions)} interactions)")
                    
                except Exception as e:
                    logger.error(f"❌ Error processing batch {batch_path}: {e}")
            
            if not all_interactions:
                logger.warning("⚠️ No interactions found in batches")
                return
            
            logger.info(f"📊 Total interactions for training: {len(all_interactions)}")
            
            # Convert to training format
            training_data = self.prepare_training_data(all_interactions)
            
            # Train models
            self.train_collaborative_model(training_data)
            self.train_content_based_model(training_data)
            
            # Mark batches as processed
            self.mark_batches_processed(processed_batch_files)
            
            logger.info("✅ Training completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Error in batch processing: {e}")
        finally:
            self.is_training = False
    
    def prepare_training_data(self, interactions):
        """Convert interactions to training format"""
        try:
            # Create DataFrames for different model types
            user_item_data = []
            content_data = []
            
            for interaction in interactions:
                # User-item interaction data for collaborative filtering
                user_item_data.append({
                    'user_id': interaction['user_id'],
                    'track_id': interaction['track_id'],
                    'rating': interaction['rating'],
                    'timestamp': interaction['timestamp']
                })
                
                # Content-based data
                track_display = interaction.get('track_display', {})
                audio_features = interaction.get('audio_features', {})
                
                content_data.append({
                    'track_id': interaction['track_id'],
                    'track_name': track_display.get('name', 'Unknown'),
                    'artist_name': track_display.get('artist_name', 'Unknown'),
                    'emotion': track_display.get('emotion', 'neutral'),
                    'popularity': track_display.get('popularity', 0),
                    **audio_features  # Expand audio features
                })
            
            # Convert to pandas DataFrames
            user_item_df = pd.DataFrame(user_item_data)
            content_df = pd.DataFrame(content_data).drop_duplicates(subset=['track_id'])
            
            logger.info(f"📊 Prepared training data:")
            logger.info(f"   👥 User-item interactions: {len(user_item_df)}")
            logger.info(f"   🎵 Unique tracks: {len(content_df)}")
            logger.info(f"   👤 Unique users: {user_item_df['user_id'].nunique()}")
            
            return {
                'user_item': user_item_df,
                'content': content_df,
                'interactions': interactions
            }
            
        except Exception as e:
            logger.error(f"❌ Error preparing training data: {e}")
            return None
    
    def mark_batches_processed(self, batch_files):
        """Mark batches as processed by moving to processed directory"""
        try:
            processed_dir = "/app/data/batches/processed"
            os.makedirs(processed_dir, exist_ok=True)
            
            for batch_path in batch_files:
                filename = os.path.basename(batch_path)
                processed_path = os.path.join(processed_dir, filename)
                
                # Copy to processed directory
                import shutil
                shutil.copy2(batch_path, processed_path)
                
                logger.info(f"✅ Marked as processed: {filename}")
                
        except Exception as e:
            logger.error(f"❌ Error marking batches as processed: {e}")
    
    def train_collaborative_model(self, training_data):
        """Train collaborative filtering model using ALS"""
        try:
            logger.info("🧠 Training collaborative filtering model...")
            
            user_item_df = training_data['user_item']
            
            if len(user_item_df) < 10:
                logger.warning("⚠️ Not enough data for collaborative training")
                return None
            
            # Simple collaborative filtering using scikit-learn
            from sklearn.decomposition import NMF
            from sklearn.preprocessing import LabelEncoder
            import numpy as np
            
            # Encode user and track IDs
            user_encoder = LabelEncoder()
            track_encoder = LabelEncoder()
            
            user_item_df['user_encoded'] = user_encoder.fit_transform(user_item_df['user_id'])
            user_item_df['track_encoded'] = track_encoder.fit_transform(user_item_df['track_id'])
            
            # Create user-item matrix
            n_users = user_item_df['user_encoded'].nunique()
            n_tracks = user_item_df['track_encoded'].nunique()
            
            user_item_matrix = np.zeros((n_users, n_tracks))
            
            for _, row in user_item_df.iterrows():
                user_item_matrix[int(row['user_encoded']), int(row['track_encoded'])] = row['rating']
            
            # Train NMF model
            model = NMF(n_components=min(10, min(n_users, n_tracks)), random_state=42)
            W = model.fit_transform(user_item_matrix)
            H = model.components_
            
            # Save model dengan metadata
            model_data = {
                'model': model,
                'user_encoder': user_encoder,
                'track_encoder': track_encoder,
                'user_features': W,
                'track_features': H,
                'n_users': n_users,
                'n_tracks': n_tracks
            }
            
            metadata = {
                'model_type': 'collaborative_nmf',
                'training_data_size': len(user_item_df),
                'n_users': n_users,
                'n_tracks': n_tracks,
                'training_timestamp': datetime.now().isoformat(),
                'performance_metrics': {
                    'reconstruction_error': model.reconstruction_err_
                }
            }
            
            model_path = self.model_manager.save_model(model_data, 'collaborative', metadata=metadata)
            logger.info(f"✅ Collaborative model trained and saved: {model_path}")
            
            return model_data
            
        except Exception as e:
            logger.error(f"❌ Error training collaborative model: {e}")
            return None

    def train_content_based_model(self, training_data):
        """Train content-based recommendation model"""
        try:
            logger.info("🎯 Training content-based model...")
            
            content_df = training_data['content']
            
            if len(content_df) < 5:
                logger.warning("⚠️ Not enough content data for training")
                return None
            
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity
            from sklearn.preprocessing import StandardScaler
            import numpy as np
            
            # Prepare features
            audio_features = ['acousticness', 'danceability', 'energy', 'instrumentalness',
                             'liveness', 'loudness', 'speechiness', 'tempo', 'valence']
            
            # Filter available audio features
            available_features = [f for f in audio_features if f in content_df.columns]
            
            if available_features:
                # Normalize audio features
                scaler = StandardScaler()
                audio_matrix = scaler.fit_transform(content_df[available_features].fillna(0))
            else:
                audio_matrix = np.zeros((len(content_df), 1))
                scaler = None
            
            # Create text features dari artist names dan emotions
            text_features = (content_df['artist_name'].fillna('unknown') + ' ' + 
                            content_df['emotion'].fillna('neutral'))
            
            # TF-IDF for text features
            tfidf = TfidfVectorizer(max_features=100, stop_words='english')
            text_matrix = tfidf.fit_transform(text_features)
            
            # Combine features
            if text_matrix.shape[1] > 0:
                combined_features = np.hstack([audio_matrix, text_matrix.toarray()])
            else:
                combined_features = audio_matrix
            
            # Calculate similarity matrix
            similarity_matrix = cosine_similarity(combined_features)
            
            # Create model data
            model_data = {
                'similarity_matrix': similarity_matrix,
                'track_ids': content_df['track_id'].tolist(),
                'track_features': combined_features,
                'scaler': scaler,
                'tfidf': tfidf,
                'feature_names': available_features
            }
            
            metadata = {
                'model_type': 'content_based_cosine',
                'n_tracks': len(content_df),
                'audio_features': available_features,
                'text_features': ['artist_name', 'emotion'],
                'training_timestamp': datetime.now().isoformat()
            }
            
            model_path = self.model_manager.save_model(model_data, 'content_based', metadata=metadata)
            logger.info(f"✅ Content-based model trained and saved: {model_path}")
            
            return model_data
            
        except Exception as e:
            logger.error(f"❌ Error training content-based model: {e}")
            return None

# Global service instance
auto_trainer = AutoRetrainingService()

# Flask API endpoints
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "service": "Auto-Retraining Service",
        "is_training": auto_trainer.is_training,
        "last_training": auto_trainer.last_training_time.isoformat() if auto_trainer.last_training_time else None,
        "queued_batches": len(auto_trainer.batch_queue)
    })

@app.route('/retrain', methods=['POST'])
def manual_retrain():
    """Manual trigger untuk retraining"""
    data = request.get_json()
    batch_id = data.get('batch_id', 'manual_trigger')
    
    logger.info(f"🔧 Manual retraining triggered for batch: {batch_id}")
    
    if auto_trainer.is_training:
        return jsonify({"message": "Training already in progress"}), 409
    
    # Add to queue
    auto_trainer.batch_queue.append({
        'batch_id': batch_id,
        'size': data.get('size', 0),
        'timestamp': datetime.now().isoformat(),
        'trigger': 'manual'
    })
    
    # Start retraining
    threading.Thread(target=auto_trainer.start_retraining, daemon=True).start()
    
    return jsonify({"message": "Retraining started", "batch_id": batch_id})

@app.route('/status', methods=['GET'])
def get_training_status():
    """Get detailed training status"""
    return jsonify({
        "is_training": auto_trainer.is_training,
        "last_training": auto_trainer.last_training_time.isoformat() if auto_trainer.last_training_time else None,
        "queued_batches": len(auto_trainer.batch_queue),
        "queue_details": auto_trainer.batch_queue
    })

@app.route('/train', methods=['POST'])
def trigger_training():
    """Manually trigger training process"""
    try:
        if auto_trainer.is_training:
            return jsonify({
                "status": "already_training",
                "message": "Training is already in progress"
            }), 400
        
        # Check for available batches
        batches = auto_trainer.get_pending_batches()
        
        if not batches:
            return jsonify({
                "status": "no_data",
                "message": "No batch data available for training"
            }), 400
        
        # Start training in background thread
        import threading
        training_thread = threading.Thread(target=auto_trainer.start_retraining)
        training_thread.daemon = True
        training_thread.start()
        
        return jsonify({
            "status": "training_started",
            "message": f"Training started with {len(batches)} batches",
            "batches_queued": len(batches),
            "estimated_duration": "5-10 minutes"
        })
        
    except Exception as e:
        logger.error(f"❌ Error triggering training: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/batches', methods=['GET'])
def list_available_batches():
    """List available batches for training"""
    try:
        batches = auto_trainer.get_pending_batches()
        processed_batches = auto_trainer.get_processed_batches()
        
        return jsonify({
            "pending_batches": len(batches),
            "processed_batches": len(processed_batches),
            "batch_details": [
                {
                    "filename": os.path.basename(batch),
                    "size": os.path.getsize(batch),
                    "created": datetime.fromtimestamp(os.path.getctime(batch)).isoformat()
                } for batch in batches[:10]  # Show last 10
            ]
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/models', methods=['GET'])
def list_trained_models():
    """List available trained models"""
    try:
        models = auto_trainer.model_manager.list_models()
        
        model_info = {}
        for model_type, model_files in models.items():
            model_info[model_type] = {
                "total_models": len(model_files),
                "latest_model": max(model_files) if model_files else None,
                "models": model_files[-5:]  # Last 5 models
            }
        
        return jsonify({
            "available_models": model_info,
            "models_path": auto_trainer.model_manager.models_path
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/train/auto', methods=['POST'])
def enable_auto_training():
    """Enable automatic training mode"""
    try:
        enable = request.json.get('enable', True) if request.is_json else True
        
        auto_trainer.auto_training_enabled = enable
        
        if enable:
            # Start auto-detection loop
            import threading
            auto_thread = threading.Thread(target=auto_trainer.auto_training_loop)
            auto_thread.daemon = True
            auto_thread.start()
        
        return jsonify({
            "auto_training": enable,
            "message": "Auto training enabled" if enable else "Auto training disabled"
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def start_kafka_listener():
    """Start Kafka listener in background thread"""
    try:
        listener_thread = threading.Thread(target=auto_trainer.listen_for_retraining_signals, daemon=True)
        listener_thread.start()
        logger.info("🎧 Kafka listener started")
    except Exception as e:
        logger.warning(f"⚠️ Kafka listener could not start: {e}")

if __name__ == '__main__':
    # Start auto-training loop
    auto_thread = threading.Thread(target=auto_trainer.auto_training_loop, daemon=True)
    auto_thread.start()
    
    # Start Kafka listener (optional)
    start_kafka_listener()
    
    logger.info("🚀 Starting Auto-Retraining Service...")
    app.run(host='0.0.0.0', port=8000, debug=False)