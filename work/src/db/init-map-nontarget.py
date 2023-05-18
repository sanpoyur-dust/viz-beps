import os
import psycopg2
import re


def create_table(cur):
    cur.execute(f"DROP TABLE IF EXISTS map_nontarget;")
    cur.execute(f"CREATE TABLE map_nontarget(guideid TEXT PRIMARY KEY);")


def insert_data(cur):
    with open("assets/amplicons/AMPLICONS_FILE_control.txt", "r") as f:
        for line in f:
            cols = line.strip().split("\t")
            values = "'" + re.sub(r"\.", "_", cols[0]) + "'"
            cur.execute(f"INSERT INTO map_nontarget (guideid) VALUES({values});")


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
