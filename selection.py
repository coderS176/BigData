from collections import defaultdict

# Step 1: Map Function


def map_function(tables):
    selected_data = []
    for table in tables:
        for row in table:
            if row["B"] <= 2:
                selected_data.append(row)
    return selected_data

# Step 2: Hash Function


def hash_function(selected_data):
    n = len(selected_data)
    part1 = selected_data[:n // 2]
    part2 = selected_data[n // 2:]
    return part1, part2

# Step 3: Swap Function


def swap_tables(part1_mp1, part2_mp1, part1_mp2, part2_mp2):
    return (part1_mp2, part2_mp1), (part2_mp2, part1_mp1)

# Step 4: Club Function


def club_values(parts):
    combined = defaultdict(list)
    for part in parts:
        for row in part:
            key = tuple(row.items())  # Create a hashable key
            combined[key].append(row)
    return [list(v)[0] for v in combined.values()]  # Remove duplicates

# Step 5: Reduce Function


def reduce_function(data):
    return data  # In selection, reducing just emits the filtered tuples


# Input data
mp1 = [[{'A': 1, 'B': 2}, {'A': 3, 'B': 1}],
       [{'A': 1, 'B': 2}, {'A': 3, 'B': 5}]]
mp2 = [[{'A': 2, 'B': 3}, {'A': 4, 'B': 5}],
       [{'A': 1, 'B': 1}, {'A': 2, 'B': 1}]]

# Execute map phase
mapped_mp1 = map_function(mp1)
mapped_mp2 = map_function(mp2)

# Execute hash phase
part1_mp1, part2_mp1 = hash_function(mapped_mp1)
part1_mp2, part2_mp2 = hash_function(mapped_mp2)

# Execute swap phase
(rw1_part1, rw2_part1), (rw1_part2, rw2_part2) = swap_tables(
    part1_mp1, part2_mp1, part1_mp2, part2_mp2)

# Combine parts for each reducer worker
rw1_combined = club_values([rw1_part1, rw1_part2])
rw2_combined = club_values([rw2_part1, rw2_part2])

print('rw1')
print(rw1_combined)

print('rw2')
print(rw2_combined)

"""
Example: 
mp1: [[{'A': 1, 'B': 2}, {'A': 3, 'B': 1}], [{'A': 1, 'B': 2}, {'A': 3, 'B': 5}]]
mp2: [[{'A': 2, 'B': 3}, {'A': 4, 'B': 5}], [{'A': 1, 'B': 1}, {'A': 2, 'B': 1}]]

Humare paas do map workers hain. Har map worker ke paas do tables hain, Hum dono map workers ko alag-alag process karenge..
Step 1: Map Worker
Har map worker apne table ko check karega aur check krega ki B<=2 hai ya nahi.
Agar condition satisfy hoti hai, toh us tuple ko include kar liya jayega.
Hum duplicate key-value pair bhi append krte jayenge.

Step 2: Hashing and Splitting
Ab hum ek hash function apply karte hain. Isse data ko do parts mein divide kiya jata hai, jaise half data ek part mein aur baaki ka doosre part mein.
Phir ek swap function apply hota hai, jo map workers ke data ko swap karta hai. swaping simple mp1 ki first table ko mp2 ki second table ke saath swap karna h

Step 3: Reduce Worker
Jab data swap ho jata hai, tab reduce worker ke paas control chala jayega.
Har reduce worker dono map workers ka data combine kar ke process karta hai.
Final mein, reduce worker sirf key emit karega.
"""
