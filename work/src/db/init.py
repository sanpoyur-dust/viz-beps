import os
import psycopg2


col_to_ty = {
    "rep": "TEXT",
    "sample": "TEXT",
    "guideid": "TEXT",
    "aligned_sequence": "TEXT",
    "read_status": "TEXT",
    "reference_sequence": "TEXT",
    "deletion_positions": "INTEGER ARRAY",
    "deletion_sizes": "INTEGER ARRAY",
    "insertion_positions": "INTEGER ARRAY",
    "insertion_sizes": "INTEGER ARRAY",
    "substitution_positions": "INTEGER ARRAY",
    "substitution_values": "CHARACTER ARRAY",
    "n_deleted": "INTEGER",
    "n_inserted": "INTEGER",
    "n_mutated": "INTEGER",
    "reads": "NUMERIC"
}
col_to_ix = dict.fromkeys(col_to_ty.keys())
keys = ", ".join(col_to_ix.keys())


def create_table(cur):
    schema = ", ".join([f"{col} {ty}" for col, ty in col_to_ty.items()])
    cur.execute(f"DROP TABLE IF EXISTS k562;")
    cur.execute(f"CREATE TABLE k562(id BIGSERIAL PRIMARY KEY, {schema});")
    
    
def insert_data(cur):
    def normalize_data(data, ty):
        data = data.strip()
        if ty == "TEXT":
            return f"'{data}'"
        elif ty == "INTEGER ARRAY":
            data = data.replace("[", "{").replace("]", "}")
            return f"'{data}'"
        elif ty == "CHARACTER ARRAY":
            data = data.replace("[", "").replace("]", "")
            data = "{" + (", ".join([chr(int(e)) for e in data.split(" ")]) if data else "") + "}"
            return f"'{data}'"
        else:
            return data
    
    with open("assets/alleleTables/K562-alleleTable.txt", "r") as f:
        cols = f.readline().strip().split("\t")            
        for ix, col in enumerate(cols):
            col = col.lower().replace("%", "")
            if col in col_to_ix:
                col_to_ix[col] = ix
                
        ty_and_ix = list(zip(col_to_ty.values(), col_to_ix.values()))
        
        for line in f:
            cols = line.strip().split("\t")
            values = ", ".join([normalize_data(cols[ix], ty) for ty, ix in ty_and_ix])
            cur.execute(f"INSERT INTO k562 ({keys}) VALUES({values});")


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
        