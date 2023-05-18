import os
import psycopg2
import re


def create_table(cur):
    cur.execute(f"DROP TABLE IF EXISTS map_label;")
    cur.execute(f"CREATE TABLE map_label(sample TEXT PRIMARY KEY, label TEXT);")


def insert_data(cur):
    with open("assets/sampleLabels.txt", "r") as f:
        for line in f:
            cols = line.strip().split("\t")
            normalize_data = ["'" + col + "'" for col in cols]
            values = ", ".join(normalize_data)
            cur.execute(
                f"INSERT INTO map_label (sample, label) VALUES({values});"
            )


if __name__ == "__main__":
    os.chdir("../..")
    with psycopg2.connect(
        host="localhost",
        port=9432,
        dbname="postgres",
        user="postgres",
        password="madison",
    ) as con:
        cur = con.cursor()
        create_table(cur)
        insert_data(cur)
