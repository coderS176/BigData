from collections import defaultdict

# Step 1: Map Function


def map_function(mp):
    """
    Emit each tuple as a key-value pair.
    The key is the tuple of the dictionary's items.
    The value is the dictionary (row) itself.
    """
    mapped_data = []
    for table in mp:
        for row in table:
            # Emit (key, value) tuple, where key is the tuple of (key, value) pairs from the dictionary
            mapped_data.append((tuple(row.items()), row))  # Emit (key, value)
    return mapped_data

# Step 2: Hash Function (For simplicity, using a simple partitioning mechanism)


def hash_function(mapped_data, num_buckets=2):
    """
    Partition data into buckets based on a simple hash of the tuple key.
    """
    buckets = [defaultdict(list) for _ in range(num_buckets)]
    for key, value in mapped_data:
        bucket_index = hash(key) % num_buckets
        buckets[bucket_index][key].append(value)
    return buckets

# Step 3: Reduce Function (Intersection)


def reducer_function(buckets):
    """
    Emit keys where values are the same for each key.
    """
    result = []
    for bucket in buckets:
        for key, values in bucket.items():
            if len(values) == 2:  # If the key appears twice (once in each table)
                # Emit the key (intersection)
                result.append(key)
    return result


# Input data
mp1 = [
    [{'A': 1, 'B': 2}, {'A': 3, 'B': 1}],  # t1 in mp1
    [{'A': 1, 'B': 2}, {'A': 2, 'B': 1}]   # t2 in mp1
]

mp2 = [
    [{'A': 2, 'B': 3}, {'A': 4, 'B': 5}],  # t1 in mp2
    [{'A': 1, 'B': 1}, {'A': 2, 'B': 1}]   # t2 in mp2
]

# Map Phase
mapped_mp1 = map_function(mp1)
mapped_mp2 = map_function(mp2)

# Hash Phase
hashed_mp1 = hash_function(mapped_mp1)
hashed_mp2 = hash_function(mapped_mp2)

# Reduce Phase (Intersection)
result_mp1 = reducer_function(hashed_mp1)
result_mp2 = reducer_function(hashed_mp2)

# Combine the results from both maps
final_intersection = result_mp1 + result_mp2

# Output the final result
print("Intersection Result:", final_intersection)
