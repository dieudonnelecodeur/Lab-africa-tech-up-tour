**Build images and run instances**

`docker-compose build`

`docker-compose up -d`

**First mount the `.sql file` [here](https://edu.postgrespro.com/demo-big-en.zip) to your postgres_etl instance**

**After that run the following command**

`psql -f demo_big_en_20170815.sql -U etl`