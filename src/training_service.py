from flask import Flask, request, jsonify
import json
import logging
import threading
import time
from datetime import datetime
from kafka import KafkaConsumer
from minio import Minio
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

class AutoRetrainingService:
    def __init__(self):
        self.minio_client = None
        self.kafka_consumer = None
        self.is_training = False
        self.last_training_time = None
        self.batch_queue = []
        
        self.setup_minio()
        self.setup_kafka_consumer()
    
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
                bootstrap_servers=['kafka:9092'],
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                group_id='training-service',
                auto_offset_reset='latest'
            )
            logger.info("✅ Kafka consumer initialized")
        except Exception as e:
            logger.error(f"❌ Kafka consumer setup failed: {e}")
    
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

def start_kafka_listener():
    """Start Kafka listener in background thread"""
    listener_thread = threading.Thread(target=auto_trainer.listen_for_retraining_signals, daemon=True)
    listener_thread.start()
    logger.info("🎧 Kafka listener started")

if __name__ == '__main__':
    # Start Kafka listener
    start_kafka_listener()
    
    logger.info("🚀 Starting Auto-Retraining Service...")
    app.run(host='0.0.0.0', port=8000, debug=False)