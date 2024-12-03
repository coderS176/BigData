from collections import defaultdict

# Step 1: Map Function


def map_function(tables, attributes):
    """
    Extracts only the desired attributes from the input tables.
    """
    projected_data = []
    for table in tables:
        for row in table:
            projected_row = {attr: row[attr]
                             for attr in attributes if attr in row}
            projected_data.append(projected_row)
    return projected_data

# Step 2: Hash Function


def hash_function(projected_data):
    """
    Splits the projected data into two parts.
    """
    n = len(projected_data)
    part1 = projected_data[:n // 2]
    part2 = projected_data[n // 2:]
    return part1, part2

# Step 3: Swap Function


def swap_tables(part1_mp1, part2_mp1, part1_mp2, part2_mp2):
    """
    Swaps the parts between map workers.
    """
    return (part1_mp2, part2_mp1), (part2_mp2, part1_mp1)

# Step 4: Club Function


def club_values(parts):
    """
    Combines the tuples into a single set of data, removing duplicates.
    """
    combined = defaultdict(list)
    for part in parts:
        for row in part:
            key = tuple(row.items())  # Create a hashable key
            combined[key].append(row)
    return [list(v)[0] for v in combined.values()]  # Remove duplicates

# Step 5: Reduce Function


def reduce_function(data):
    """
    Outputs the final set of projected tuples.
    """
    return data


# Input data
mp1 = [
    [{'A': 1, 'B': 2, 'C': 3}, {'A': 3, 'B': 1, 'C': 4}],
    [{'A': 2, 'B': 2, 'C': 3}, {'A': 4, 'B': 3, 'C': 4}]
]
mp2 = [
    [{'A': 5, 'B': 7, 'C': 2}, {'A': 7, 'B': 1, 'C': 1}],
    [{'A': 3, 'B': 2, 'C': 5}, {'A': 4, 'B': 1, 'C': 0}]
]

# Desired attributes for projection
attributes = ['B', 'C']

# Execute map phase
mapped_mp1 = map_function(mp1, attributes)
mapped_mp2 = map_function(mp2, attributes)

# Execute hash phase
part1_mp1, part2_mp1 = hash_function(mapped_mp1)
part1_mp2, part2_mp2 = hash_function(mapped_mp2)

# Execute swap phase
(rw1_part1, rw2_part1), (rw1_part2, rw2_part2) = swap_tables(
    part1_mp1, part2_mp1, part1_mp2, part2_mp2)

# Combine parts for each reducer worker
rw1_combined = club_values([rw1_part1, rw1_part2])
rw2_combined = club_values([rw2_part1, rw2_part2])

# Combine reducer workers and apply reduce function
final_combined = rw1_combined + rw2_combined
result = reduce_function(final_combined)

# Output the final result
print("Final Result:", result)


"""
In relational algebra, the projection operation retrieves specific attributes (columns) from the tuples in the dataset.
 For this task, the MapReduce paradigm can be used to achieve the projection by:

Map Function: Select only the desired attributes from the tuples in each table.
Hash Function: Distribute the projected tuples across parts.
Swap Function: Swap the parts between map workers to balance the data.
Club Data: Combine the data for each map worker, removing duplicates.
Reduce Function: Merge all the projected data and output the final result.

Example:
mp1 = [[{'A':1,'B':2:'C':3},{'A':3,'B':1,'C':4}],[{'A'2,'B':2,'C':3},{'A':4,'B':3,'C':4}]]
mp2 = [[{'A':5,'B'7,'C':2},{'A':7,'B':1,'C':1}],[{'A':3,'B':2,'C':5},{'A':4,'B':1,'C':0}]]
"""
