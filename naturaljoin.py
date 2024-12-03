from collections import defaultdict

# Step 1: Map function


def map_function(mapped_data, source):
    """
    Emit key-value pairs for each table ('t1' or 't2').
    For 't1': emit (B, ('t1', A))
    For 't2': emit (B, ('t2', C))
    """
    result = []
    for table, rows in mapped_data.items():
        if table == 't1':
            for row in rows:
                result.append((row['B'], ('t1', row['A'])))
        elif table == 't2':
            for row in rows:
                result.append((row['B'], ('t2', row['C'])))
    return result

# Step 2: Hash function


def hash_function(mapped_data, num_buckets=3):
    """
    Partition data into buckets based on the join column's key (B).
    """
    buckets = [defaultdict(list) for _ in range(num_buckets)]
    for key, value in mapped_data:
        bucket_index = hash(key) % num_buckets
        buckets[bucket_index][key].append(value)
    return buckets

# Step 3: Reduce function


def reduce_function(buckets):
    """
    Perform the natural join: combine values from 't1' and 't2' for the same key.
    """
    result = []
    for bucket in buckets:
        for key, values in bucket.items():
            # Separate values into list_t1 and list_t2
            list_t1 = [a for (source, a) in values if source == 't1']
            list_t2 = [c for (source, c) in values if source == 't2']

            # Generate all combinations of (A, B, C)
            for a in list_t1:
                for c in list_t2:
                    # Emit the joined row
                    result.append({'A': a, 'B': key, 'C': c})
    return result


# Input data
mp1 = {
    't1': [{'A': 1, 'B': 2}, {'A': 2, 'B': 3}, {'A': 5, 'B': 6}],
    't2': [{'B': 2, 'C': 3}, {'B': 4, 'C': 4}, {'B': 6, 'C': 1}]
}
mp2 = {
    't1': [{'A': 6, 'B': 1}, {'A': 6, 'B': 3}, {'A': 7, 'B': 6}],
    't2': [{'B': 9, 'C': 8}, {'B': 3, 'C': 4}, {'B': 7, 'C': 1}]
}

# Map Phase
mapped_mp1 = map_function(mp1, source='mp1')
mapped_mp2 = map_function(mp2, source='mp2')

# Combine mapped data
mapped_data = mapped_mp1 + mapped_mp2

# Hash Phase
hashed_data = hash_function(mapped_data)

# Reduce Phase
join_result = reduce_function(hashed_data)

# Output the result
print("Natural Join Result:", join_result)


"""
map(key, value):
    if key == R:
        for (a,b) in value:
            emit(b, (R,a))

    else:
        for (b,c) in value:
            emit(b, (S,c))
reduce(key, values):
    list_R = [a for (x,a) in values if x == R]
    list_S = [c for (x,c) in values if x == S]
    for a in list_R:
        for c in list_S:
            emit(key, (a,key,c))
Here R is Table1 and S is Table2. The map function emits the key-value pairs based on the source table. The reduce function performs the natural join by iterating over the values for each key and combining the values from Table1 and Table2 based on the common key. The emit function outputs the joined data as (key, value) pairs.
"""
