from collections import defaultdict

# Step 1: Map Function
def map_function(map_worker):
    """
    Emit each tuple as a key-value pair.
    """
    mapped_data = []
    for table in map_worker:
        for row in table:
            mapped_data.append((tuple(row.items()), row))  # Emit (key, value)
    return mapped_data

# Step 2: Hash Function
def hash_function(mapped_data, num_buckets=2):
    """
    Partition data into buckets based on a simple hash of the tuple key.
    """
    buckets = [defaultdict(list) for _ in range(num_buckets)]
    for key, value in mapped_data:
        bucket_index = hash(key) % num_buckets
        buckets[bucket_index][key].append(value)
    return buckets

# Step 3: Swap Function
def swap_function(buckets1, buckets2):
    """
    Swap partitions between map workers to balance processing.
    """
    swapped_buckets1 = buckets2[:]
    swapped_buckets2 = buckets1[:]
    return swapped_buckets1, swapped_buckets2

# Step 4: Reduce Function
def reducer_function(buckets):
    """
    Combine tuples in each bucket to remove duplicates.
    """
    result = set()
    for bucket in buckets:
        for key in bucket.keys():
            result.add(key)
    return [dict(k) for k in result]  # Convert keys back to dictionaries

# Input data
mp1 = [
    [{'A': 1, 'B': 2}, {'A': 3, 'B': 1}],  # t1 in mp1
    [{'A': 1, 'B': 2}, {'A': 3, 'B': 5}]   # t2 in mp1
]
mp2 = [
    [{'A': 2, 'B': 3}, {'A': 4, 'B': 5}],  # t1 in mp2
    [{'A': 1, 'B': 1}, {'A': 2, 'B': 3}]   # t2 in mp2
]

# Map Phase
mapped_mp1 = map_function(mp1)
mapped_mp2 = map_function(mp2)

# Hash Phase
hashed_mp1 = hash_function(mapped_mp1)
hashed_mp2 = hash_function(mapped_mp2)

# Swap Phase
swapped_mp1, swapped_mp2 = swap_function(hashed_mp1, hashed_mp2)

# Reduce Phase
result_mp1 = reducer_function(swapped_mp1)
result_mp2 = reducer_function(swapped_mp2)

# Combine results from all map workers
final_union = result_mp1 + result_mp2

# Remove duplicates across workers
final_union = {tuple(row.items()): row for row in final_union}.values()  # Deduplicate

# Output the final result
print("Union Result:", list(final_union))


"""
algo:
map(key,value):
  for tuple in value:
    emit(tuple, tuple)

reduce(key, value):
    emit(key, key)
"""