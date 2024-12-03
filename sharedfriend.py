from collections import defaultdict

# Sample data: friendships
data = {
    'A': ['B', 'C', 'D'],
    'B': ['A', 'C', 'E'],
    'C': ['A', 'B', 'F'],
    'D': ['A'],
    'E': ['B'],
    'F': ['C']
}


# Map function
def map_friends(data):
    pairs = []
    for person, friends in data.items():
        for friend in friends:
            # Create a sorted pair to ensure mutual friendship representation
            pair = tuple(sorted([person, friend]))
            pairs.append((pair, set(friends)))
    return pairs


# Reduce function
def reduce_shared_friends(mapped_data):
    shared_friends = defaultdict(set)

    # Group friends for each pair
    for pair, friends in mapped_data:
        shared_friends[pair] |= friends  # Union of friend lists

    # Compute intersection of friends for each pair
    result = {}
    for pair, friends in shared_friends.items():
        # Intersect the friend sets of both individuals in the pair
        result[pair] = friends.intersection(data[pair[0]], data[pair[1]])
    return result


# Execute MapReduce
mapped = map_friends(data)
shared_friends_result = reduce_shared_friends(mapped)

# Display results
for pair, friends in shared_friends_result.items():
    print(f"Shared friends between {pair[0]} and {pair[1]}: {sorted(friends)}")
