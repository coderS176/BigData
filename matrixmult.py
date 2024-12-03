from functools import reduce


def matrix_multiply(A, B):
    # Ensure matrices A and B are valid for multiplication
    rows_A, cols_A = len(A), len(A[0])
    rows_B, cols_B = len(B), len(B[0])

    if cols_A != rows_B:
        raise ValueError(
            "Number of columns in A must equal the number of rows in B.")

    # Transpose B for easier column access
    B_transposed = list(map(list, zip(*B)))

    # Helper function to compute a single row-column product
    def compute_element(row, col):
        return sum(a * b for a, b in zip(row, col))

    # Compute the result matrix
    result = [[compute_element(row, col) for col in B_transposed] for row in A]

    return result


A = [[1, 2, 3], [4, 5, 6]]
B = [[7, 8], [9, 10], [11, 12]]
result = matrix_multiply(A, B)

for row in result:
    print(row)
