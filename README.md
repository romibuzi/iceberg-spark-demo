Iceberg Spark demo
======

Demo of how to use Apache Iceberg table format from a Spark application.

## Requirements

- Docker Compose
- Java >= 11
- IDE like IntelliJ

## Steps

Start both MinIO object storage and PostgreSQL server with `docker-compose`:

```
docker-compose up -d
```

- Minio admin console will be accessible at http://127.0.0.1:9001. login: `minioadmin`. password: `minioadmin`.
- PostgreSQL server will be available at 127.0.0.1:5432 (`postgres`/`postgres`). Database is `iceberg_db`.

Import the project in an IDE (project is based on Maven and Java >= 11) 
and run [IcebergWithSpark.scala](src/main/scala/com/romibuzi/IcebergWithSpark.scala). 
You can edit and launch methods you want in this file.
