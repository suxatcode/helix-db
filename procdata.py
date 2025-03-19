# note: there are no duplicate ids (I tested this)

import os
import numpy as np
import pandas as pd
from tqdm import tqdm
from scipy.spatial.distance import cdist
import huggingface_hub
import time
import shutil

def download_dataset(dataset_path, repo_id="KShivendu/dbpedia-entities-openai-1M"):
    """Download dataset from Hugging Face if it doesn't exist locally."""
    print(f"Downloading dataset from {repo_id} to {dataset_path}...")
    huggingface_hub.snapshot_download(
        repo_id=repo_id,
        local_dir=dataset_path,
        repo_type="dataset"
    )
    print("Download complete!")

def load_parquet_files(data_dir):
    """Load all parquet files from the directory into a single DataFrame."""
    print(f"Loading parquet files from {data_dir}...")
    
    # Find all parquet files in the directory
    parquet_files = [f for f in os.listdir(data_dir) if f.endswith('.parquet')]
    
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")
    
    # Load and concatenate all parquet files
    dfs = []
    for file in tqdm(parquet_files, desc="Loading files"):
        file_path = os.path.join(data_dir, file)
        df = pd.read_parquet(file_path)
        dfs.append(df)
    
    # Concatenate all dataframes
    return pd.concat(dfs, ignore_index=True)

def find_top_k_similar_vectors(embeddings, ids, k=100, batch_size=1000):
    """
    Find top k similar vectors for each vector in the dataset using batched processing.
    
    Args:
        embeddings: numpy array of embeddings
        ids: list of corresponding ids
        k: number of neighbors to find
        batch_size: number of vectors to process in each batch
    
    Returns:
        DataFrame with columns 'id', 'similar_ids' containing the top k similar vector ids
    """
    n = embeddings.shape[0]
    results = []
    
    print("poop")
    for i in tqdm(range(0, n, batch_size), desc="Processing batches"):
        # Define the current batch
        end_idx = min(i + batch_size, n)
        batch_embeddings = embeddings[i:end_idx]
        
        # Calculate distances from this batch to all embeddings
        # Using Euclidean distance
        distances = cdist(batch_embeddings, embeddings, metric='euclidean')
        
        # For each vector in the batch, get the indices of the k+1 closest vectors (including itself)
        for j, dist_row in enumerate(distances):
            # Get top k+1 indices sorted by distance (closest first)
            closest_indices = np.argsort(dist_row)[:k+1]
            
            # Skip the first index if it's the vector itself (0 distance)
            if closest_indices[0] == i + j:
                closest_indices = closest_indices[1:k+1]
            else:
                closest_indices = closest_indices[:k]
            
            # Get the IDs of the closest vectors
            vector_id = ids[i + j]
            similar_ids = [ids[idx] for idx in closest_indices]
            
            results.append({'id': vector_id, 'similar_ids': similar_ids})
            
        # Free up memory
        del distances
    
    return pd.DataFrame(results)

def main():
    # Define directory for data
    data_dir = "data"
    
    # Create directory if it doesn't exist
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        download_dataset(data_dir)
    
    # Check if the directory is empty (no parquet files)
    parquet_files = [f for f in os.listdir(data_dir) if f.endswith('.parquet')]
    if not parquet_files:
        download_dataset(data_dir)
    
    # Load all parquet files
    df = load_parquet_files(data_dir)
    
    print(f"Loaded {len(df)} vectors with embeddings")
    
    # Extract embeddings and IDs
    embeddings = np.stack(df['openai'].values)
    ids = df['_id'].values
    
    print(f"Embedding shape: {embeddings.shape}")
    
    # Find top 100 similar vectors for each vector
    print("Finding top 100 similar vectors for each vector...")
    similarity_df = find_top_k_similar_vectors(embeddings, ids, k=100)
    
    # Save results to CSV
    output_file = "ground_truths.csv"
    print(f"Saving results to {output_file}...")
    similarity_df.to_csv(output_file, index=False)
    
    print("Done!")

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")