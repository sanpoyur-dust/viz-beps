import os
import psycopg2
import re


def create_table(cur):
    cur.execute(f"DROP TABLE IF EXISTS map_strand;")
    cur.execute(
        f"CREATE TABLE map_strand(guideid TEXT PRIMARY KEY, is_positive BOOLEAN);"
    )


def insert_data(cur):
    with open("assets/amplicons/AMPLICONS_FILE_strandMap.txt", "r") as f:
        for line in f:
            cols = line.strip().split("\t")
            normalize_data = [None] * 2
            normalize_data[0] = "'" + re.sub(r"\.", "_", cols[0]) + "'"
            normalize_data[1] = "true" if cols[1] == "1" else "false"
            values = ", ".join(normalize_data)
            cur.execute(
                f"INSERT INTO map_strand (guideid, is_positive) VALUES({values});"
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
