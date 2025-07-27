#!/usr/bin/env python3

try:
    import cupy as cp
    CUDA_AVAILABLE = True
except ImportError:
    # Fallback to numpy if cupy is not available
    import numpy as cp
    CUDA_AVAILABLE = False
    print("Warning: CuPy not available, falling back to CPU computation with NumPy")

import numpy as np
import struct
from typing import Tuple, List
import os
from tqdm import tqdm
import math

def read_fbin_file(filename: str) -> np.ndarray:
    """Read vectors from .fbin format file (keep on CPU)"""
    with open(filename, 'rb') as f:
        # Read header: num_vectors (4 bytes) + dimension (4 bytes)
        header = f.read(8)
        num_vectors, dimension = struct.unpack('<II', header)
        
        print(f"Reading {filename}: {num_vectors} vectors of dimension {dimension}")
        
        # Read all vector data as float32
        data = np.frombuffer(f.read(), dtype=np.float32)
        
        # Reshape to (num_vectors, dimension)
        vectors = data.reshape(num_vectors, dimension)
        
        return vectors

def write_fbin_file(filename: str, data: np.ndarray):
    """Write data to .fbin format file"""
    with open(filename, 'wb') as f:
        # Write header
        num_vectors, dimension = data.shape
        header = struct.pack('<II', num_vectors, dimension)
        f.write(header)
        
        # Write data as float32
        f.write(data.astype(np.float32).tobytes())

def write_ibin_file(filename: str, data: np.ndarray):
    """Write data to .ibin format file (for neighbor indices)"""
    with open(filename, 'wb') as f:
        # Write header
        num_vectors, dimension = data.shape
        header = struct.pack('<II', num_vectors, dimension)
        f.write(header)
        
        # Write data as int32
        f.write(data.astype(np.int32).tobytes())

def read_ibin_file(filename: str) -> np.ndarray:
    """Read vectors from .ibin format file (integer data)"""
    with open(filename, 'rb') as f:
        # Read header: num_vectors (4 bytes) + dimension (4 bytes)
        header = f.read(8)
        num_vectors, dimension = struct.unpack('<II', header)
        
        print(f"Reading {filename}: {num_vectors} vectors of dimension {dimension}")
        
        # Read all vector data as int32
        data = np.frombuffer(f.read(), dtype=np.int32)
        
        # Reshape to (num_vectors, dimension)
        vectors = data.reshape(num_vectors, dimension)
        
        return vectors


def compute_ground_truth_distances_and_neighbors(base_vectors: np.ndarray, query_vectors: np.ndarray, k: int = 100, num_threads: int = None) -> Tuple[np.ndarray, np.ndarray]:
    """Compute ground truth k-NN squared distances and neighbor indices using optimized GPU acceleration (or CPU fallback)"""
    num_queries = query_vectors.shape[0]
    num_base = base_vectors.shape[0]
    dim = base_vectors.shape[1]
    
    print(f"Computing ground truth for {num_queries} queries against {num_base} base vectors")
    print(f"Using k={k} nearest neighbors")
    print("Computing squared distances to match existing format")
    
    if CUDA_AVAILABLE:
        print(f"GPU device: {cp.cuda.runtime.getDevice()}")
        
        # Get available GPU memory
        gpu_mem_total = cp.cuda.runtime.memGetInfo()[1]
        gpu_mem_available = cp.cuda.runtime.memGetInfo()[0]
        print(f"GPU memory - Total: {gpu_mem_total / (1024**3):.2f} GB, Available: {gpu_mem_available / (1024**3):.2f} GB")
    else:
        print("Using CPU computation (NumPy fallback)")
    
    # Pre-compute norms (keep on CPU for now, transfer later if needed)
    print("Pre-computing vector norms...")
    base_norms = np.sum(base_vectors * base_vectors, axis=1, dtype=np.float32)
    query_norms = np.sum(query_vectors * query_vectors, axis=1, dtype=np.float32)
    
    # Optimize memory usage - calculate optimal batch sizes
    if CUDA_AVAILABLE:
        # Memory needed: base_chunk + base_norms + query_batch + query_norms + distance_matrix
        bytes_per_base = dim * 4 + 4  # vector + norm
        bytes_per_query = dim * 4 + 4  # vector + norm
        
        # Use conservative memory allocation based on what worked before
        # Use 60% of available memory for safety
        available_memory = int(gpu_mem_available * 0.6)
        
        # Balance base chunk size and query batch size for GPU memory
        # Use conservative memory allocation - try different combinations
        for try_base_chunk in [1000000, 1500000, 2000000]:  # 1M, 1.5M, 2M
            try_base_chunk = min(try_base_chunk, num_base)
            base_mem = try_base_chunk * bytes_per_base
            remaining_mem = available_memory - base_mem
            distance_mem_per_query = try_base_chunk * 4
            max_query_batch = remaining_mem // (bytes_per_query + distance_mem_per_query)
            
            if max_query_batch >= 200:  # Need at least 200 for good GPU utilization
                base_chunk_size = try_base_chunk
                query_batch_size = min(max_query_batch, 500, num_queries)  # Cap at 500 for safety
                query_batch_size = max(200, query_batch_size)
                break
        else:
            # Fallback to very conservative sizes
            base_chunk_size = min(500000, num_base)  # 500k base vectors
            query_batch_size = min(100, num_queries)  # Small query batches
    else:
        # For CPU computation, optimize based on available system memory
        import psutil
        available_memory = psutil.virtual_memory().available
        
        # Estimate memory per vector (assuming float32)
        bytes_per_vector = dim * 4
        
        # Use 50% of available memory for computation, leaving room for OS and other processes
        usable_memory = int(available_memory * 0.5)
        
        # Calculate optimal batch sizes
        # For base vectors: balance memory usage to allow larger query batches
        # Reduce base chunk size to allow more memory for query batches
        max_base_vectors = usable_memory // (bytes_per_vector * 6)  # Factor of 6 for more conservative base chunks
        base_chunk_size = min(max_base_vectors, 1000000, num_base)  # Cap at 1M for better query batch sizes
        
        # For query vectors: calculate based on distance matrix memory requirements
        # Distance matrix is the largest memory consumer: query_batch_size * base_chunk_size * 4 bytes
        remaining_memory = usable_memory - (base_chunk_size * bytes_per_vector)
        
        # Be more conservative - distance matrix dominates memory usage
        distance_matrix_memory = base_chunk_size * 4  # 4 bytes per distance per query
        max_query_batch = remaining_memory // (bytes_per_vector + distance_matrix_memory)
        
        # More reasonable caps now that base chunks are smaller
        query_batch_size = min(max_query_batch, 1000, num_queries)  # Cap at 1K for balance
        query_batch_size = max(100, query_batch_size)  # At least 100 for good vectorization
        
        # For very large datasets, still be conservative but allow larger batches
        if num_base > 5000000:  # 5M+ base vectors
            query_batch_size = min(query_batch_size, 500)
        if num_queries > 5000:  # 5K+ queries
            query_batch_size = min(query_batch_size, 300)
        
        print(f"Available memory: {available_memory / (1024**3):.2f} GB, Using: {usable_memory / (1024**3):.2f} GB")
    
    print(f"Optimized batch sizes - Base: {base_chunk_size}, Query: {query_batch_size}")
    
    # Store final results on CPU
    ground_truth_distances = np.zeros((num_queries, k), dtype=np.float32)
    ground_truth_neighbors = np.zeros((num_queries, k), dtype=np.int32)
    
    # Process queries in batches first (outer loop)
    for query_start in tqdm(range(0, num_queries, query_batch_size), desc="Query batches"):
        query_end = min(query_start + query_batch_size, num_queries)
        current_batch_size = query_end - query_start
        
        # Load query batch (transfer to device - GPU if available, otherwise stays on CPU)
        batch_queries = query_vectors[query_start:query_end]
        batch_query_norms = query_norms[query_start:query_end]
        
        queries_gpu = cp.asarray(batch_queries)
        query_norms_gpu = cp.asarray(batch_query_norms)
        
        # Initialize tracking for this query batch - optimize for neighbor computation
        # Scale candidates based on number of chunks to ensure accuracy
        num_base_chunks = math.ceil(num_base / base_chunk_size)
        
        # SMART 100% ACCURACY: Use adaptive thresholding based on distance bounds
        # Keep enough candidates to guarantee we don't lose any true top-k
        
        # Use k*2 candidates as requested (200 for k=100)
        # This is much more efficient and should still give good accuracy
        candidates_multiplier = 2
        
        # Calculate candidates needed per chunk
        initial_candidates = k * candidates_multiplier
        
        print(f"Smart accuracy optimization: {num_base_chunks} chunks, starting with {initial_candidates} candidates")
        
        all_distances = []
        all_indices = []
        
        # Track the kth smallest distance seen so far across all chunks
        # This helps us prune candidates that can't possibly be in top-k
        current_kth_distance = cp.inf if CUDA_AVAILABLE else np.inf
        
        # Process base vectors in chunks for this query batch (already calculated above)
        
        for chunk_idx in range(num_base_chunks):
            base_start = chunk_idx * base_chunk_size
            base_end = min(base_start + base_chunk_size, num_base)
            chunk_size = base_end - base_start
            
            # Load base chunk (transfer to device - GPU if available, otherwise stays on CPU)
            base_chunk_gpu = cp.asarray(base_vectors[base_start:base_end])
            base_norms_chunk_gpu = cp.asarray(base_norms[base_start:base_end])
            
            # Compute distances using optimized GEMM
            # This is the most computationally intensive part - optimize it
            dot_products = cp.dot(queries_gpu, base_chunk_gpu.T)
            
            # Use broadcasting for distance computation (most efficient)
            # Ensure numerical stability by clipping negative distances to 0
            distances_chunk = (base_norms_chunk_gpu[cp.newaxis, :] + 
                             query_norms_gpu[:, cp.newaxis] - 
                             2 * dot_products)
            
            # Fix numerical precision issues that can cause tiny negative distances
            distances_chunk = cp.maximum(distances_chunk, 0)
            
            # SMART FILTERING: For first chunk, keep many candidates
            # For subsequent chunks, only keep candidates better than current kth distance
            if chunk_idx == 0:
                # First chunk: keep initial_candidates
                candidates_per_chunk = min(initial_candidates, chunk_size)
            else:
                # Subsequent chunks: still keep many candidates for 100% accuracy
                # Don't reduce too much or we'll lose accuracy
                candidates_per_chunk = min(initial_candidates, chunk_size)
            
            # If chunk is small, keep everything
            if chunk_size <= candidates_per_chunk:
                num_candidates = chunk_size
                top_distances = distances_chunk
                
                # More efficient index computation for keeping all vectors
                if not hasattr(cp, '_base_indices_cache') or getattr(cp, '_base_indices_cache_size', 0) != chunk_size:
                    cp._base_indices_cache = cp.arange(chunk_size, dtype=cp.int32)
                    cp._base_indices_cache_size = chunk_size
                
                top_global_indices = cp._base_indices_cache[cp.newaxis, :] + base_start
                top_global_indices = cp.broadcast_to(top_global_indices, (current_batch_size, chunk_size))
            else:
                # Use argpartition to get top candidates
                num_candidates = candidates_per_chunk
                top_indices = cp.argpartition(distances_chunk, num_candidates-1, axis=1)[:, :num_candidates]
                
                # Extract distances more efficiently
                query_indices = cp.arange(current_batch_size)[:, cp.newaxis]
                top_distances = distances_chunk[query_indices, top_indices]
                
                # Convert local indices to global base vector indices
                top_global_indices = top_indices + base_start
            
            # Transfer to CPU immediately to free GPU memory (or keep on CPU if no GPU)
            all_distances.append(cp.asnumpy(top_distances))
            all_indices.append(cp.asnumpy(top_global_indices))
            
            # Free memory aggressively
            del base_chunk_gpu, base_norms_chunk_gpu, dot_products, distances_chunk, top_distances, top_global_indices
            if chunk_size > candidates_per_chunk:
                del top_indices
            
            # Free GPU memory pool if using CUDA
            if CUDA_AVAILABLE:
                cp.get_default_memory_pool().free_all_blocks()
        
        # Free query memory
        del queries_gpu, query_norms_gpu
        if CUDA_AVAILABLE:
            cp.get_default_memory_pool().free_all_blocks()
        
        # Combine all chunks and get final top-k on CPU using vectorized operations
        all_distances_combined = np.concatenate(all_distances, axis=1)
        all_indices_combined = np.concatenate(all_indices, axis=1)
        
        # Vectorized top-k selection for the entire batch
        if all_distances_combined.shape[1] > k:
            # Use argpartition for efficient batch top-k selection
            top_k_indices = np.argpartition(all_distances_combined, k-1, axis=1)[:, :k]
            
            # Vectorized indexing to get distances and neighbors
            batch_indices = np.arange(current_batch_size)[:, np.newaxis]
            top_k_distances = all_distances_combined[batch_indices, top_k_indices]
            top_k_neighbors = all_indices_combined[batch_indices, top_k_indices]
        else:
            top_k_distances = all_distances_combined
            top_k_neighbors = all_indices_combined
        
        # Vectorized sorting for the entire batch
        sort_indices = np.argsort(top_k_distances, axis=1)
        batch_indices = np.arange(current_batch_size)[:, np.newaxis]
        
        # Apply sorting to both distances and neighbors in one operation
        ground_truth_distances[query_start:query_end] = top_k_distances[batch_indices, sort_indices]
        ground_truth_neighbors[query_start:query_end] = top_k_neighbors[batch_indices, sort_indices]
        
        # Clear CPU memory
        del all_distances, all_distances_combined, all_indices, all_indices_combined
    
    return ground_truth_distances, ground_truth_neighbors

def main():
    import sys
    import argparse
    
    global CUDA_AVAILABLE
    
    parser = argparse.ArgumentParser(description='Fast GPU ground truth computation with exact precision')
    parser.add_argument('--dataset-file', required=True, help='Path to base vectors .fbin file')
    parser.add_argument('--queries-file', required=True, help='Path to query vectors .fbin file')
    parser.add_argument('--num-queries', type=int, required=True, help='Number of queries to process')
    parser.add_argument('--distances-output', help='Output ground truth distances .fbin filename')
    parser.add_argument('--neighbors-output', help='Output ground truth neighbors .ibin filename')
    parser.add_argument('--reference-distances', help='Optional reference distances file for validation')
    parser.add_argument('--reference-neighbors', help='Optional reference neighbors file for validation')
    parser.add_argument('--k', type=int, default=100, help='Number of nearest neighbors (default: 100)')
    parser.add_argument('--threads', type=int, help='Number of threads to use (default: auto-detect)')
    parser.add_argument('--device', choices=['cpu', 'gpu', 'auto'], default='auto', 
                       help='Device to use: cpu, gpu, or auto (default: auto)')
    
    args = parser.parse_args()
    
    # Validate that at least one output is specified
    if not args.distances_output and not args.neighbors_output:
        parser.error("At least one output must be specified: --distances-output or --neighbors-output")
    
    # Handle device selection based on --device flag
    if args.device == 'cpu':
        CUDA_AVAILABLE = False
        print("Device set to CPU (forced)")
    elif args.device == 'gpu':
        if not CUDA_AVAILABLE:
            print("Error: GPU requested but CuPy is not available. Install CuPy to use GPU.")
            sys.exit(1)
        elif not cp.cuda.is_available():
            print("Error: GPU requested but no CUDA GPU detected.")
            sys.exit(1)
        else:
            print(f"Device set to GPU: {cp.cuda.runtime.getDeviceProperties(0)['name'].decode()}")
    else:  # args.device == 'auto'
        # Check if GPU is available and show appropriate message
        if CUDA_AVAILABLE and cp.cuda.is_available():
            print(f"Auto-detected CUDA device: {cp.cuda.runtime.getDeviceProperties(0)['name'].decode()}")
        elif CUDA_AVAILABLE:
            print("Warning: CuPy is available but no CUDA GPU detected, falling back to CPU computation")
            CUDA_AVAILABLE = False
        else:
            print("Using CPU computation (CuPy not available)")
    
    print(f"Loading base vectors from {args.dataset_file}...")
    base_vectors = read_fbin_file(args.dataset_file)
    
    print(f"Loading query vectors from {args.queries_file}...")
    query_vectors = read_fbin_file(args.queries_file)
    
    # Limit to specified number of queries
    if args.num_queries > len(query_vectors):
        print(f"Warning: Requested {args.num_queries} queries but only {len(query_vectors)} available")
        args.num_queries = len(query_vectors)
    
    print(f"Computing ground truth distances and neighbors for {args.num_queries} queries...")
    selected_queries = query_vectors[:args.num_queries]
    ground_truth_distances, ground_truth_neighbors = compute_ground_truth_distances_and_neighbors(base_vectors, selected_queries, k=args.k, num_threads=args.threads)
    
    if args.distances_output:
        print(f"Saving ground truth distances to {args.distances_output}...")
        write_fbin_file(args.distances_output, ground_truth_distances)
    
    if args.neighbors_output:
        print(f"Saving ground truth neighbors to {args.neighbors_output}...")
        write_ibin_file(args.neighbors_output, ground_truth_neighbors)
    
    # Validate distances if reference file is provided
    if args.reference_distances:
        print(f"Loading reference distances from {args.reference_distances}...")
        try:
            reference_distances = read_fbin_file(args.reference_distances)
            reference_distances_subset = reference_distances[:args.num_queries]
            
            print("Verifying distances against reference...")
            max_diff = np.max(np.abs(reference_distances_subset - ground_truth_distances))
            mean_diff = np.mean(np.abs(reference_distances_subset - ground_truth_distances))
            
            if np.allclose(reference_distances_subset, ground_truth_distances, rtol=1e-5, atol=1e-5):
                print(f"✓ SUCCESS: Computed distances match reference for {args.num_queries} queries")
                print(f"Max difference: {max_diff:.2e}, Mean difference: {mean_diff:.2e}")
            else:
                print("✗ Distance arrays differ significantly")
                print(f"Max difference: {max_diff}")
                print(f"Mean difference: {mean_diff}")
                
                # Show sample values for debugging
                print(f"\nFirst query - Reference distances: {reference_distances_subset[0][:5]}")
                print(f"First query - Computed distances: {ground_truth_distances[0][:5]}")
                print(f"Difference: {reference_distances_subset[0][:5] - ground_truth_distances[0][:5]}")
        except Exception as e:
            print(f"Warning: Could not validate distances against reference file: {e}")
    
    # Validate neighbors if reference file is provided
    if args.reference_neighbors:
        print(f"Loading reference neighbors from {args.reference_neighbors}...")
        try:
            reference_neighbors = read_ibin_file(args.reference_neighbors)
            reference_neighbors_subset = reference_neighbors[:args.num_queries]
            
            print("Verifying neighbors against reference...")
            matches = np.array_equal(reference_neighbors_subset, ground_truth_neighbors)
            
            if matches:
                print(f"✓ SUCCESS: Computed neighbors match reference exactly for {args.num_queries} queries")
            else:
                print("✗ Neighbor arrays differ")
                
                # Calculate average accuracy across all query-neighbor pairs
                # Count matches for each neighbor position across all queries
                matches_per_position = reference_neighbors_subset == ground_truth_neighbors
                total_matches = np.sum(matches_per_position)
                total_pairs = args.num_queries * args.k
                average_accuracy = total_matches / total_pairs * 100
                
                # Also count perfect queries for reference
                query_matches = np.all(matches_per_position, axis=1)
                num_exact_matches = np.sum(query_matches)
                
                print(f"Average accuracy: {average_accuracy:.2f}% ({total_matches}/{total_pairs} neighbor matches)")
                print(f"Perfect queries: {num_exact_matches}/{args.num_queries} ({num_exact_matches/args.num_queries*100:.1f}%)")
                
                # Show sample values for debugging
                print(f"\nFirst query - Reference neighbors: {reference_neighbors_subset[0][:10]}")
                print(f"First query - Computed neighbors: {ground_truth_neighbors[0][:10]}")
                
                # Check if differences might be due to ties in distances
                if not query_matches[0]:
                    ref_dists = reference_distances_subset[0] if args.reference_distances else None
                    comp_dists = ground_truth_distances[0]
                    if ref_dists is not None:
                        print(f"First query - Reference distances: {ref_dists[:10]}")
                    print(f"First query - Computed distances: {comp_dists[:10]}")
        except Exception as e:
            print(f"Warning: Could not validate neighbors against reference file: {e}")
    
    if not args.reference_distances and not args.reference_neighbors:
        print("No reference files provided, skipping validation.")
    
    output_files = []
    if args.distances_output:
        output_files.append(args.distances_output)
    if args.neighbors_output:
        output_files.append(args.neighbors_output)
    print(f"Ground truth computation complete. Output saved to: {', '.join(output_files)}")

if __name__ == "__main__":
    main()