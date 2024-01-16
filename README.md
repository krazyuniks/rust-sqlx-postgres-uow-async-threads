# TODOs Example

## Setup

1. Create .env file if needed. Run `docker-compose up -d` to run Postgres in the background.

2. Declare the database URL

    ```
    export DATABASE_URL="postgres://postgres:password@localhost/todos"
    ```

3. Create the database.

    ```
    $ sqlx db create
    ```

4. Run sql migrations

    ```
    $ sqlx migrate run
    ```

## Usage

Add a todo 

```
cargo run -- add "todo description"
```

Complete a todo.

```
cargo run -- done <todo id>
```

List all todos

```
cargo run
```
