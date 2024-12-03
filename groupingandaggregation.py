from collections import defaultdict

# Step 1: Map function


def map_function(data):
    grouped_data = defaultdict(list)
    for table in data.values():
        for row in table:
            key = (row["A"], row["B"])
            grouped_data[key].append(row["C"])
    return grouped_data

# Step 2: Hash function


def hash_function(mapped_data):
    keys = list(mapped_data.keys())
    n = len(keys)
    part1 = {k: mapped_data[k] for k in keys[:n // 2]}
    part2 = {k: mapped_data[k] for k in keys[n // 2:]}
    return part1, part2

# Step 3: Swap function


def swap_tables(part1_mp1, part2_mp1, part1_mp2, part2_mp2):
    return (part1_mp2, part2_mp1), (part2_mp2, part1_mp1)

# Step 4: Club function


def club_values(data):
    combined = defaultdict(list)
    for part in data:
        for key, values in part.items():
            combined[key].extend(values)
    return combined

# Step 5: Reduce function


def reduce_function(data):
    return {key: max(values) for key, values in data.items()}


# Input data
mp1 = {
    "t1": [{"A": 1, "B": 2, "C": 10, "D": 2}, {"A": 2, "B": 3, "C": 2, "D": 8}, {"A": 1, "B": 2, "C": 9, "D": 6}],
    "t2": [{"A": 4, "B": 2, "C": 7, "D": 9}, {"A": 6, "B": 8, "C": 3, "D": 3}, {"A": 3, "B": 2, "C": 5, "D": 7}]
}
mp2 = [{
    "t1": [{"A": 1, "B": 2, "C": 1, "D": 4}, {"A": 2, "B": 2, "C": 8, "D": 7}, {"A": 1, "B": 3, "C": 9, "D": 1}],
    "t2": [{"A": 3, "B": 2, "C": 5, "D": 6}, {"A": 2, "B": 2, "C": 4, "D": 2}, {"A": 3, "B": 4, "C": 2, "D": 3}]
}]

# Execute map phase
mapped_mp1 = map_function(mp1)
mapped_mp2 = map_function(mp2[0])

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
final_combined = {**rw1_combined, **rw2_combined}
result = reduce_function(final_combined)

for key, value in result.items():
    print("key:", key, "value:", value)


"""
mp1 = { 
    "t1": [{"A": 1, "B": 2, "C": 10, "D": 2},
           {"A": 2, "B": 3, "C": 2, "D": 8},
           {"A": 1, "B": 2, "C": 9, "D": 6}],
    "t2": [{"A": 4, "B": 2, "C": 7, "D": 9},
           {"A": 6, "B": 8, "C": 3, "D": 3},
           {"A": 3, "B": 2, "C": 5, "D": 7}]
}

mp2= [{ 
    "t1": [{"A": 1, "B": 2, "C": 1, "D": 4},
           {"A": 2, "B": 2, "C": 8, "D": 7},
           {"A": 1, "B": 3, "C": 9, "D": 1}],
    "t2": [{"A": 3, "B": 2, "C": 5, "D": 6},
           {"A": 2, "B": 2, "C": 8, "D": 7},
           {"A": 3, "B": 4, "C": 2, "D": 3}]
}]

so we have 2 mapworker and inside each mapworker we have 2 tables.
we have to group by A and B and apply max as aggregation function on C.
so first we have to perform map function in which we have to group by A and B and append C values for repeated keys it merge the data of 2 tables.
after this we have to perform hash function in which each mapworker will splitted in 2 tables first have n/2 rows and second have n/2 rows.
after mapping we have 5 rows so we make first table with 3 and second with 2 rows.
after this we swap the tables between mapworker1 and mapworker2 to reduce the duplication of data. in this we swap 1st table of first mapworker with 2nd table of second mapworker and vice versa.
after this we club the duplicated key values in mapworker1 and mapworker2.
after that we club values of both mapworker tables into single key value pair table.
note swaping part will be done by reducer worker so mp1 and mp2 will be passed to reducer worker.
and it get converted in rw1, rw2.
after this we have to apply max aggregation on C for each (A, B) so we have to perform reduce function.
this will be our final result.
"""
