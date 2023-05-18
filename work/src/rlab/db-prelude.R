library(RPostgreSQL)

con <- dbConnect(PostgreSQL(),
                 host="db",
                 port="5432",
                 dbname="postgres",
                 user= "postgres",
                 password="madison")
