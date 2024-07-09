# Build images and run instances

`docker-compose build`
`docker-compose up -d`

# First mount the `.sql file` to your postgres_etl instance 

# After that run the following command

`psql -f docker-entrypoint-initdb.d/demo_big_en_20170815.sql -U etl`

# Now we'll create our Data pipeline