from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import rand, when, col, lit
import logging
import os
import json

class SongRecommendationTrainer:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("SongRecommendationTraining") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.logger = logging.getLogger(__name__)
        
    def load_all_datasets(self):
        """Load SEMUA dataset dengan comprehensive NaN handling"""
        from storage.minio_client import MinIOClient
        
        minio_client = MinIOClient()
        datasets = {}
        
        # Base path untuk container
        base_path = '/app/data/raw'
        
        try:
            # 1. Load Spotify Tracks dengan robust cleaning
            tracks_path = f'{base_path}/spotify_tracks.csv'
            if os.path.exists(tracks_path):
                # Read with proper options
                tracks_df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "false") \
                    .option("nullValue", "") \
                    .option("nanValue", "NaN") \
                    .csv(tracks_path)
                
                # Cast numeric columns dengan error handling
                numeric_columns = {
                    'acousticness': 'double',
                    'danceability': 'double', 
                    'energy': 'double',
                    'instrumentalness': 'double',
                    'liveness': 'double',
                    'loudness': 'double',
                    'speechiness': 'double',
                    'tempo': 'double',
                    'valence': 'double',
                    'popularity': 'double',
                    'duration_ms': 'double'
                }
                
                # Cast dengan try-catch individual
                for col_name, data_type in numeric_columns.items():
                    if col_name in tracks_df.columns:
                        try:
                            tracks_df = tracks_df.withColumn(col_name, col(col_name).cast(data_type))
                        except Exception as e:
                            self.logger.warning(f"⚠️ Could not cast {col_name}: {e}")
                
                # Comprehensive null/NaN replacement
                robust_defaults = {
                    'acousticness': 0.0,
                    'danceability': 0.5,
                    'energy': 0.5,
                    'instrumentalness': 0.0,
                    'liveness': 0.1,
                    'loudness': -10.0,
                    'speechiness': 0.05,
                    'tempo': 120.0,
                    'valence': 0.5,
                    'popularity': 0.0,
                    'duration_ms': 180000.0
                }
                
                # Replace problematic values
                from pyspark.sql.functions import isnan, isnull, when, lit
                
                for col_name, default_val in robust_defaults.items():
                    if col_name in tracks_df.columns:
                        tracks_df = tracks_df.withColumn(
                            col_name,
                            when(
                                isnull(col(col_name)) | isnan(col(col_name)) |
                                (col(col_name) == lit("")) | (col(col_name) == lit("NULL")) |
                                (col(col_name) == float('inf')) | (col(col_name) == float('-inf')),
                                lit(default_val)
                            ).otherwise(col(col_name))
                        )
                
                datasets['tracks'] = tracks_df
                self.logger.info(f"✅ Loaded {tracks_df.count()} tracks with robust cleaning")
                
                # Debug schema
                self.logger.info("✅ Tracks schema after type casting:")
                for col_name in ['acousticness', 'danceability', 'energy', 'popularity']:
                    if col_name in tracks_df.columns:
                        dtype = dict(tracks_df.dtypes)[col_name]
                        self.logger.info(f"  {col_name}: {dtype}")
                
            else:
                self.logger.error(f"❌ Tracks file not found: {tracks_path}")
            
            # 2. Load Artists dengan proper types
            artists_path = f'{base_path}/spotify_artists.csv'
            if os.path.exists(artists_path):
                artists_df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(artists_path)
                
                # Cast numeric columns
                if 'followers' in artists_df.columns:
                    artists_df = artists_df.withColumn('followers', col('followers').cast("long"))
                if 'popularity' in artists_df.columns:
                    artists_df = artists_df.withColumn('popularity', col('popularity').cast("double"))
                    
                datasets['artists'] = artists_df
                self.logger.info(f"✅ Loaded {artists_df.count()} artists")
            
            # 3. Load Albums dengan proper types
            albums_path = f'{base_path}/spotify_albums.csv'
            if os.path.exists(albums_path):
                albums_df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(albums_path)
                
                if 'total_tracks' in albums_df.columns:
                    albums_df = albums_df.withColumn('total_tracks', col('total_tracks').cast("int"))
                    
                datasets['albums'] = albums_df
                self.logger.info(f"✅ Loaded {albums_df.count()} albums")
            
            # 4. Load Emotion Data
            emotion_files = [f for f in os.listdir(base_path) if f.endswith('.json')]
            if emotion_files:
                emotion_path = f'{base_path}/{emotion_files[0]}'
                emotion_df = self.spark.read.json(emotion_path)
                datasets['emotions'] = emotion_df
                self.logger.info(f"✅ Loaded {emotion_df.count()} emotion records")
            
            self.logger.info(f"📊 Total datasets loaded: {len(datasets)}")
            return datasets
            
        except Exception as e:
            self.logger.error(f"❌ Error loading datasets: {e}")
            raise
    
    def create_enriched_tracks_dataset(self, datasets):
        """Combine datasets dengan proper join handling"""
        
        tracks_df = datasets.get('tracks')
        artists_df = datasets.get('artists')
        albums_df = datasets.get('albums')
        emotions_df = datasets.get('emotions')
        
        if tracks_df is None:
            raise ValueError("Tracks dataset is required but not found")
        
        enriched_df = tracks_df
        
        # Join dengan artists - fix column name conflict
        if artists_df is not None:
            try:
                # Rename conflicting columns before join
                artists_select = artists_df.select(
                    col('id').alias('artist_ref_id'),
                    'followers',
                    'genres',
                    col('popularity').alias('artist_popularity')
                )
                
                if 'artists_id' in tracks_df.columns:  # Check actual column name
                    enriched_df = enriched_df.join(
                        artists_select,
                        tracks_df.artists_id == artists_select.artist_ref_id,
                        'left'
                    )
                    self.logger.info("✅ Joined with artists data")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not join artists data: {e}")
        
        # Join dengan albums - fix column conflict
        if albums_df is not None:
            try:
                # Rename conflicting columns
                albums_select = albums_df.select(
                    col('id').alias('album_ref_id'),
                    'release_date',
                    'total_tracks'
                )
                
                if 'album_id' in tracks_df.columns:
                    enriched_df = enriched_df.join(
                        albums_select,
                        tracks_df.album_id == albums_select.album_ref_id,
                        'left'
                    )
                    self.logger.info("✅ Joined with albums data")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not join albums data: {e}")
        
        # Join dengan emotion data
        if emotions_df is not None:
            try:
                if 'song' in emotions_df.columns and 'name' in tracks_df.columns:
                    emotion_features = emotions_df.select(
                        'song',
                        'artist', 
                        'emotion',
                        'variance',
                        col('Genre').alias('emotion_genre')  # Avoid conflicts
                    )
                    enriched_df = enriched_df.join(
                        emotion_features,
                        tracks_df.name == emotion_features.song,
                        'left'
                    )
                    self.logger.info("✅ Joined with emotion data")
            except Exception as e:
                self.logger.warning(f"⚠️ Could not join emotion data: {e}")
        
        self.logger.info(f"📊 Enriched dataset created with {enriched_df.count()} records")
        return enriched_df
    
    def content_based_features(self, df):
        """Extract features dengan thorough NaN handling"""
        
        # Audio features yang akan digunakan
        base_audio_features = ['acousticness', 'danceability', 'energy', 'instrumentalness', 
                              'liveness', 'loudness', 'speechiness', 'tempo', 'valence']
        
        available_features = []
        
        # Debug: Check current data types
        self.logger.info("🔍 Checking column data types:")
        for col_name in base_audio_features:
            if col_name in df.columns:
                current_dtype = dict(df.dtypes)[col_name]
                self.logger.info(f"  {col_name}: {current_dtype}")
                
                # Force cast to double if needed
                if current_dtype != 'double':
                    self.logger.info(f"  🔄 Casting {col_name} from {current_dtype} to double")
                    df = df.withColumn(col_name, col(col_name).cast("double"))
                
                available_features.append(col_name)
    
        # Add additional numeric features
        additional_features = ['popularity', 'duration_ms']
        for feature in additional_features:
            if feature in df.columns:
                current_dtype = dict(df.dtypes)[feature]
                if current_dtype != 'double':
                    df = df.withColumn(feature, col(feature).cast("double"))
                available_features.append(feature)
        
        if not available_features:
            raise ValueError("No valid features found")
        
        self.logger.info(f"✅ Using {len(available_features)} features: {available_features}")
        
        # CRUCIAL: Handle NaN, null, and infinite values
        from pyspark.sql.functions import isnan, isnull, when, lit
        
        # Replace NaN and infinite values with appropriate defaults
        feature_defaults = {
            'acousticness': 0.0,
            'danceability': 0.5,
            'energy': 0.5,
            'instrumentalness': 0.0,
            'liveness': 0.1,
            'loudness': -10.0,
            'speechiness': 0.05,
            'tempo': 120.0,
            'valence': 0.5,
            'popularity': 0.0,
            'duration_ms': 180000.0
        }
        
        for feature in available_features:
            default_value = feature_defaults.get(feature, 0.0)
            
            # Replace NaN, null, and infinite values
            df = df.withColumn(
                feature,
                when(
                    isnull(col(feature)) | isnan(col(feature)) | 
                    (col(feature) == float('inf')) | (col(feature) == float('-inf')),
                    lit(default_value)
                ).otherwise(col(feature))
            )
        
        # Additional safety: Remove any remaining problematic rows
        self.logger.info("🧹 Cleaning problematic values...")
        
        # Filter out rows where any feature is still null or NaN
        clean_conditions = []
        for feature in available_features:
            clean_conditions.append(col(feature).isNotNull())
            clean_conditions.append(~isnan(col(feature)))
        
        # Combine all conditions with AND
        from functools import reduce
        from pyspark.sql.functions import col as spark_col
        
        if clean_conditions:
            final_condition = reduce(lambda a, b: a & b, clean_conditions)
            df = df.filter(final_condition)
        
        self.logger.info(f"📊 Clean dataset: {df.count()} records")
        
        # Additional validation: Check for remaining NaN values
        for feature in available_features:
            nan_count = df.filter(isnan(col(feature)) | isnull(col(feature))).count()
            if nan_count > 0:
                self.logger.warning(f"⚠️ Still found {nan_count} NaN/null values in {feature}")
    
        # Vector assembler dengan strict validation
        assembler = VectorAssembler(
            inputCols=available_features,
            outputCol="audio_features",
            handleInvalid="skip"  # Skip invalid rows
        )
        
        # Standard scaler
        scaler = StandardScaler(
            inputCol="audio_features",
            outputCol="scaled_features",
            withMean=True,
            withStd=True
        )
        
        # Build dan apply pipeline dengan error handling
        try:
            pipeline = Pipeline(stages=[assembler, scaler])
            model = pipeline.fit(df)
            result_df = model.transform(df)
            
            # Verify no NaN in final features
            final_count = result_df.filter(col("scaled_features").isNotNull()).count()
            self.logger.info(f"✅ Features extracted successfully: {final_count} valid records")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"❌ Error in feature extraction: {e}")
            
            # Emergency fallback: Use only most reliable features
            safe_features = ['popularity', 'duration_ms']
            safe_available = [f for f in safe_features if f in df.columns]
            
            if safe_available:
                self.logger.info(f"🚨 Fallback: Using only safe features: {safe_available}")
                assembler_safe = VectorAssembler(
                    inputCols=safe_available,
                    outputCol="audio_features",
                    handleInvalid="skip"
                )
                scaler_safe = StandardScaler(
                    inputCol="audio_features", 
                    outputCol="scaled_features"
                )
                pipeline_safe = Pipeline(stages=[assembler_safe, scaler_safe])
                model_safe = pipeline_safe.fit(df)
                return model_safe.transform(df)
            else:
                raise e
    
    def train_clustering_model(self, df):
        """Train K-Means clustering dengan ALL available features"""
        
        # Prepare features
        df_features = self.content_based_features(df)
        
        # Determine optimal number of clusters based on data size
        data_count = df_features.count()
        k = min(max(10, data_count // 1000), 50)  # Between 10-50 clusters
        
        # K-Means clustering
        kmeans = KMeans(
            featuresCol="scaled_features",
            predictionCol="cluster",
            k=k,
            seed=42
        )
        
        model = kmeans.fit(df_features)
        clustered_df = model.transform(df_features)
        
        # Save model
        model.write().overwrite().save("/app/data/models/kmeans_model")
        
        self.logger.info(f"K-Means clustering model trained with {k} clusters and saved")
        return model, clustered_df
    
    def train_all_models(self):
        """Train models menggunakan SEMUA dataset"""
        self.logger.info("🚀 Starting comprehensive model training...")
        
        # Load semua datasets
        datasets = self.load_all_datasets()
        
        # Create enriched dataset
        enriched_df = self.create_enriched_tracks_dataset(datasets)
        
        # Train clustering
        kmeans_model, clustered_df = self.train_clustering_model(enriched_df)
        
        # Train collaborative filtering  
        als_model, rmse = self.train_collaborative_filtering(enriched_df)
        
        # Save comprehensive training metadata
        metadata = {
            "datasets_used": list(datasets.keys()),
            "total_records": enriched_df.count(),
            "kmeans_clusters": kmeans_model.getK(),
            "als_rmse": rmse,
            "features_used": [col for col in enriched_df.columns if col in ['acousticness', 'danceability', 'energy', 'instrumentalness', 'liveness', 'loudness', 'speechiness', 'tempo', 'valence', 'popularity']],
            "training_timestamp": str(self.spark.sql("SELECT current_timestamp()").collect()[0][0])
        }
        
        with open("/app/data/models/training_metadata.json", "w") as f:
            json.dump(metadata, f, indent=2)
        
        self.logger.info("🎉 All models trained successfully with comprehensive dataset!")
        
        return {
            "kmeans_model": kmeans_model,
            "als_model": als_model,
            "rmse": rmse,
            "datasets_used": list(datasets.keys()),
            "total_records": enriched_df.count()
        }
    
    # Keep existing methods...
    def create_user_interaction_data(self, df):
        """Generate synthetic user interaction data untuk ALS"""
        
        num_users = 1000
        
        # Generate interactions based on song popularity and randomness
        interactions = df.select("id", "popularity") \
            .withColumn("user_id", (rand() * num_users).cast("int")) \
            .withColumn("rating", 
                when(col("popularity") > 70, 5.0)
                .when(col("popularity") > 50, 4.0)  
                .when(col("popularity") > 30, 3.0)
                .when(col("popularity") > 10, 2.0)
                .otherwise(1.0)
            ) \
            .withColumn("rating", col("rating") + (rand() - 0.5))
        
        return interactions
    
    def train_collaborative_filtering(self, df):
        """Train ALS model untuk collaborative filtering"""
        
        interactions = self.create_user_interaction_data(df)
        
        indexer = StringIndexer(inputCol="id", outputCol="track_index", handleInvalid="skip")
        interactions_indexed = indexer.fit(interactions).transform(interactions)
        
        train_data, test_data = interactions_indexed.randomSplit([0.8, 0.2], seed=42)
        
        als = ALS(
            userCol="user_id",
            itemCol="track_index", 
            ratingCol="rating",
            rank=50,
            maxIter=10,
            regParam=0.1,
            coldStartStrategy="drop"
        )
        
        model = als.fit(train_data)
        
        predictions = model.transform(test_data)
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
        rmse = evaluator.evaluate(predictions)
        
        self.logger.info(f"ALS Model RMSE: {rmse}")
        
        model.write().overwrite().save("/app/data/models/als_model")
        indexer.fit(interactions).write().overwrite().save("/app/data/models/track_indexer")
        
        return model, rmse

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    trainer = SongRecommendationTrainer()
    results = trainer.train_all_models()
    
    print("🤖 Comprehensive Training completed!")
    print(f"📊 Datasets used: {results['datasets_used']}")
    print(f"📊 Total records: {results['total_records']}")
    print(f"📊 ALS RMSE: {results['rmse']}")