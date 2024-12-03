from collections import defaultdict

# Input data
mp1 = [
    [{'A': 1, 'B': 2}, {'A': 3, 'B': 1}],  # t1 in mp1
    [{'A': 1, 'B': 2}, {'A': 3, 'B': 5}]   # t2 in mp1
]
mp2 = [
    [{'A': 2, 'B': 3}, {'A': 4, 'B': 5}],  # t1 in mp2
    [{'A': 1, 'B': 1}, {'A': 2, 'B': 1}]   # t2 in mp2
]

# Map function


def map_function(data, source):
    """
    Maps each dictionary row into a tuple key and associates it with a source label.
    """
    mapped_data = []
    for row in data:
        if not isinstance(row, dict):  # Ensure the row is a dictionary
            raise ValueError(f"Expected dictionary but got: {
                             type(row)}. Data: {row}")
        # Convert the dictionary to a tuple key for grouping
        key = tuple(row.values())
        mapped_data.append((key, source))  # Emit (key, source)
    return mapped_data

# Hash function


def hash_function(mapped_data, num_buckets=2):
    """
    Partitions mapped data into buckets based on the hash of the tuple key.
    """
    buckets = [defaultdict(list) for _ in range(num_buckets)]
    for key, source in mapped_data:
        bucket_index = hash(key) % num_buckets
        buckets[bucket_index][key].append(source)
    return buckets

# Reduce function


def reduce_function(buckets):
    """
    Reduces the hashed data by removing tuples present in both T1 and T2.
    Outputs the remaining tuples grouped by map worker.
    """
    results = []
    for bucket in buckets:
        for key, sources in bucket.items():
            if sources == ['T1']:  # Only emit tuples present exclusively in T1
                results.append(key)
    return results


# Process the data
# Map Phase
mapped_r_mp1 = map_function(mp1[0], source='T1')  # t1 of mp1
mapped_s_mp1 = map_function(mp1[1], source='T2')  # t2 of mp1
mapped_r_mp2 = map_function(mp2[0], source='T1')  # t1 of mp2
mapped_s_mp2 = map_function(mp2[1], source='T2')  # t2 of mp2

# Combine mapped data
mapped_data_mp1 = mapped_r_mp1 + mapped_s_mp1
mapped_data_mp2 = mapped_r_mp2 + mapped_s_mp2

# Hash Phase
hashed_mp1 = hash_function(mapped_data_mp1)
hashed_mp2 = hash_function(mapped_data_mp2)

# Reduce Phase
result_mp1 = reduce_function(hashed_mp1)
result_mp2 = reduce_function(hashed_mp2)

# Output the results
print("mp1 result:", result_mp1)
print("mp2 result:", result_mp2)
