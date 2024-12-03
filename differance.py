from collections import defaultdict

# Step 1: Map Function


def map_function(data, source):
    """
    Emit key-value pairs for each tuple in the input data, tagged by the source.
    """
    mapped_data = []
    for row in data:
        # frozenset makes tuples hashable
        mapped_data.append((frozenset(row.items()), source))
    return mapped_data

# Step 2: Hash Function


def hash_function(mapped_data, num_buckets=2):
    """
    Partition data into buckets using a hash function.
    """
    buckets = [defaultdict(list) for _ in range(num_buckets)]
    for key, value in mapped_data:
        bucket_index = hash(key) % num_buckets
        buckets[bucket_index][key].append(value)
    return buckets

# Step 3: Reduce Function


def reduce_function(buckets):
    """
    Perform the difference operation: retain tuples that exist only in R (t1).
    """
    result = []
    for bucket in buckets:
        for key, values in bucket.items():
            sources = set(values)  # Collect unique sources
            if sources == {'R'}:  # Retain tuples present only in R
                result.append(dict(key))  # Convert back to dictionary
    return result


# Input Data
mp1 = [
    [{'A': 1, 'B': 2}, {'A': 3, 'B': 1}],  # t1 in mp1
    [{'A': 1, 'B': 2}, {'A': 2, 'B': 1}]   # t2 in mp1
]
mp2 = [
    [{'A': 2, 'B': 3}, {'A': 4, 'B': 5}],  # t1 in mp2
    [{'A': 1, 'B': 1}, {'A': 2, 'B': 1}]   # t2 in mp2
]

# Map Phase (Separate map-workers for each input)
mapped_r_mp1 = map_function(mp1[0], source='R')  # t1 of mp1
mapped_s_mp1 = map_function(mp1[1], source='S')  # t2 of mp1

mapped_r_mp2 = map_function(mp2[0], source='R')  # t1 of mp2
mapped_s_mp2 = map_function(mp2[1], source='S')  # t2 of mp2

# Combine all mapped data for each map worker
mapped_data_mp1 = mapped_r_mp1 + mapped_s_mp1
mapped_data_mp2 = mapped_r_mp2 + mapped_s_mp2

# Hash Phase for each map worker
hashed_buckets_mp1 = hash_function(mapped_data_mp1, num_buckets=2)
hashed_buckets_mp2 = hash_function(mapped_data_mp2, num_buckets=2)

# Reduce Phase for each map worker
difference_result_mp1 = reduce_function(hashed_buckets_mp1)
difference_result_mp2 = reduce_function(hashed_buckets_mp2)

# Output map-worker-wise result
print("mp1:")
for result in difference_result_mp1:
    print(result)

print("mp2:")
for result in difference_result_mp2:
    print(result)

"""
Notion of difference relational algebra

Ex:
mp1 = [[{'A': 1, 'B': 2}, {'A': 3, 'B': 1}],[{'A': 1, 'B': 2}, {'A': 2, 'B': 1}]]
mp2 = [[{'A': 2, 'B': 3}, {'A': 4, 'B': 5}],[{'A': 1, 'B': 1}, {'A': 2, 'B': 1}]]

map(key, value):
    if key == R:
        for tuple in value:
            emit(tuple,R)
    else:
        for tuple in value:
            emit(tuple,S)
reduce(key, value):
    if value==[R]:
        emit(key,key)
"""
