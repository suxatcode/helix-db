import os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import torch
import pandas as pd
from tqdm import tqdm
from datasets import load_dataset

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f'Using device: {device}')

def load_vectors(n_vectors_to_load: int = 1_000_000) -> tuple[np.ndarray, list]:
    if n_vectors_to_load > 1_000_000:
        raise ValueError('cant load more than 1,000,000 vectors from this dataset')

    print(f'vectors to load: {n_vectors_to_load}')

    data_dir = 'data/'
    os.makedirs(data_dir, exist_ok=True)

    parquet_files = [f for f in os.listdir(data_dir) if f.endswith('.parquet')]
    if not parquet_files:
        print(f'no Parquet files found in {data_dir}. downloading from Hugging Face...')
        dataset = load_dataset('KShivendu/dbpedia-entities-openai-1M', split='train')

        df = dataset.to_pandas()
        total_rows = len(df)
        shard_size = total_rows // 26

        for i in tqdm(range(26), desc="saving dataset to parquet files"):
            start_idx = i * shard_size
            end_idx = min((i + 1) * shard_size, total_rows) if i < 25 else total_rows
            shard_df = df[start_idx:end_idx]
            shard_table = pa.Table.from_pandas(shard_df)
            pq.write_table(shard_table, os.path.join(data_dir, f"file{i+1}.parquet"))

        parquet_files = sorted([f for f in os.listdir(data_dir) if f.endswith('.parquet')])
        print(f'downloaded and saved {len(parquet_files)} parquet files to {data_dir}')
    else:
        print(f'found {len(parquet_files)} parquet files in {data_dir}')

    vectors = []
    id_list = []
    total_loaded = 0

    for file_name in tqdm(parquet_files, desc='loading parquet files'):
        if total_loaded >= n_vectors_to_load:
            break

        file_path = os.path.join(data_dir, file_name)
        table = pq.read_table(file_path)
        embeddings = np.stack(table['openai'].to_numpy())
        ids = table['_id'].to_pandas().tolist()

        remaining = n_vectors_to_load - total_loaded
        n_to_take = min(len(ids), remaining)
        vectors.append(embeddings[:n_to_take])
        id_list.extend(ids[:n_to_take])
        total_loaded += n_to_take

    vectors = np.vstack(vectors).astype(np.float32)
    print(f'loaded {len(id_list)} vectors with dimension {vectors.shape[1]}')
    return vectors, id_list

def compute_nearest_neighbors(n_vectors_to_load: int = 1000000):
    vectors, id_list = load_vectors(n_vectors_to_load)
    vectors_tensor = torch.tensor(vectors).to(device)
    n_vectors = vectors_tensor.shape[0]

    batch_size = 1000 # based on gpu memory
    nearest_ids_list = []

    for start in tqdm(range(0, n_vectors, batch_size), desc='computing nearest neighbors'):
        end = min(start + batch_size, n_vectors)
        batch_vectors = vectors_tensor[start:end]

        distances = torch.cdist(batch_vectors, vectors_tensor, p=2)

        distances_topk, indices = torch.topk(distances, k=101, largest=False, dim=1)

        distances_topk = distances_topk[:, 1:101]
        indices = indices[:, 1:101]

        for i in range(distances_topk.shape[0]):
            dists = distances_topk[i]
            idxs = indices[i]

            sorted_indices = idxs[torch.argsort(dists)]
            sorted_indices = sorted_indices.cpu().numpy()

            neighbor_ids = [id_list[idx] for idx in sorted_indices]
            nearest_ids_list.append(neighbor_ids)

    df = pd.DataFrame({
        '_id': id_list,
        'nearest_ids': nearest_ids_list
    })
    df['nearest_ids'] = df['nearest_ids'].apply(lambda x: ','.join(map(str, x)))

    print('saving results to csv file...')
    df.to_csv('dpedia_openai_ground_truths.csv', index=False)
    print("output saved to 'dpedia_openai_ground_truths.csv'")

if __name__ == '__main__':
    compute_nearest_neighbors()