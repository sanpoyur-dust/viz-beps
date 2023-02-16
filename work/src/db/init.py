import os
import sqlite3


col_to_ix = dict.fromkeys(
    [
        "rep",
        "sample",
        "guideID",
        "Aligned_Sequence",
        "Read_Status",
        "Reference_Sequence",
        "deletion_positions",
        "deletion_sizes",
        "insertion_positions",
        "insertion_sizes",
        "substitution_positions",
        "substitution_values",
        "n_deleted",
        "n_inserted",
        "n_mutated",
        "Reads",
    ]
)


def create_table(cur):
    schema = ", ".join([col + " TEXT" for col in col_to_ix.keys()])
    cur.execute(f"DROP TABLE IF EXISTS K562;")
    cur.execute(f"CREATE TABLE K562({schema});")


def insert_data(cur):
    with open("assets/alleleTables/K562-alleleTable.txt", "r") as f:
        cols = f.readline().strip().split("\t")
        for ix, col in enumerate(cols):
            if col == "%Reads":
                col = "Reads"
            if col in col_to_ix:
                col_to_ix[col] = ix
        for line in f:
            cols = line.strip().split("\t")
            values = ", ".join(["'" + cols[ix] + "'" for ix in col_to_ix.values()])
            cur.execute(f"INSERT INTO K562 VALUES({values});")


if __name__ == "__main__":
    os.chdir("../..")
    with sqlite3.connect("assets/alleleTables/K562-alleleTable.db.tmp") as con:
        cur = con.cursor()
        create_table(cur)
        insert_data(cur)
