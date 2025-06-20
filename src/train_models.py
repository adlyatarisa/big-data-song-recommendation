import logging
import os
import sys

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from spark.training import SongRecommendationTrainer

def main():
    print("🤖 Starting ML Training Pipeline...")
    
    try:
        # Initialize trainer
        trainer = SongRecommendationTrainer()
        
        # Train all models
        results = trainer.train_all_models()
        
        print("✅ Training completed successfully!")
        print(f"📊 Model Performance:")
        print(f"   - ALS RMSE: {results['rmse']:.4f}")
        print(f"   - K-Means Clusters: 20")
        print(f"📁 Models saved to: ../data/models/")
        
    except Exception as e:
        print(f"❌ Training failed: {e}")
        
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()