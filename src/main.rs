use async_trait::async_trait;
use sqlx::{query, Transaction};
use tokio::task;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn_str = String::from("postgresql://postgres:password@localhost/todos");
    let conn_pool = sqlx::PgPool::connect(&conn_str).await;

    match conn_pool {
        Ok(_) => println!("Connection established"),
        Err(e) => panic!("Connection error: {}", e),
    }

    let pool = conn_pool.unwrap();

    let _ = query!(r"TRUNCATE TABLE todos").execute(&pool).await?;
    let _ = query!(r"TRUNCATE TABLE users").execute(&pool).await?;

    // test thread safe
    let mut handles = Vec::new();

    //let pool = pool.clone();
    for y in 1..=300 {
        let mut uow: Transaction<'static, sqlx::Postgres> = pool.begin().await.unwrap();

        handles.push(task::spawn(async move {
            println!("after spawn : {}", y);
            let mut todo_repo = TodoRepo::new(&mut uow).await.unwrap();

            println!("inside ythread : {}", y);
            for z in 1..=10 {
                let loop_id = z + (y - 1) * 10;
                todo_repo
                    .insert(Todo {
                        id: loop_id,
                        description: format!("test {} inside {}", loop_id, y),
                    })
                    .await
                    .unwrap();
            }

            let mut user_repo = UserRepo::new(&mut uow).await.unwrap();
            for user_z in 1..=10 {
                println!("LOOP user_z : {}", user_z);
                let user_loop_id = user_z + (y - 1) * 10;
                user_repo
                    .insert(User {
                        id: user_loop_id,
                        name: format!("name:{}", user_loop_id),
                    })
                    .await
                    .unwrap();
            }

            match uow.commit().await {
                Ok(_) => println!("Transaction committed"),
                Err(e) => println!("Transaction error: {}", e),
            };
        }));
    }

    for h in handles {
        println!("after awaiting h {:#?}", h);
        h.await?;
    }

    let inserted_todo = query!(r#"SELECT id FROM todos WHERE id = $1"#, 1)
        .fetch_one(&pool)
        .await;
    assert!(inserted_todo.is_ok());

    let todos: Vec<Todo> = sqlx::query_as!(Todo, r"SELECT id, description FROM todos")
        .fetch_all(&pool)
        .await?;

    todos.iter().for_each(|todo| {
        print!("{:#?}", todo);
    });

    let inserted_user = query!(r#"SELECT id, name FROM users WHERE id = $1"#, 1)
        .fetch_one(&pool)
        .await;
    assert!(inserted_user.is_ok());

    let users: Vec<User> = sqlx::query_as!(User, r"SELECT id, name FROM users")
        .fetch_all(&pool)
        .await?;

    users.iter().for_each(|user| {
        print!("{:#?}", user);
    });

    Ok(())
}

#[derive(sqlx::FromRow, Debug)]
struct Todo {
    id: i64,
    description: String,
}
struct TodoRepo<'a> {
    tx: &'a mut Transaction<'static, sqlx::Postgres>,
}

#[derive(sqlx::FromRow, Debug)]
struct User {
    id: i64,
    name: String,
}
struct UserRepo<'a> {
    tx: &'a mut Transaction<'static, sqlx::Postgres>,
}

#[async_trait]
trait RepoTrait<'a, T, U = Self> {
    async fn new(
        tx: &'a mut Transaction<'static, sqlx::Postgres>,
    ) -> Result<U, Box<dyn std::error::Error + Send + Sync>>;
    async fn insert(&mut self, entity: T) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn get(&mut self, id: i64) -> Result<T, Box<dyn std::error::Error + Send + Sync>>;
    async fn get_all(&mut self) -> Result<Vec<T>, Box<dyn std::error::Error + Send + Sync>>;
}

#[async_trait]
impl<'a> RepoTrait<'a, Todo, TodoRepo<'a>> for TodoRepo<'a> {
    async fn new(
        tx: &'a mut Transaction<'static, sqlx::Postgres>,
    ) -> Result<TodoRepo<'a>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(TodoRepo { tx })
    }

    async fn insert(&mut self, todo: Todo) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        query!(
            r#"INSERT INTO todos (id, description)
        VALUES ( $1, $2 )
        "#,
            todo.id,
            todo.description
        )
        // In 0.7, `Transaction` can no longer implement `Executor` directly,
        // so it must be dereferenced to the internal connection type.
        .execute(&mut **self.tx)
        .await?;

        println!("insert(): {:?}", todo);
        Ok(())
    }

    async fn get(&mut self, id: i64) -> Result<Todo, Box<dyn std::error::Error + Send + Sync>> {
        let row = query!(r#"SELECT id, description FROM todos WHERE id = $1"#, id)
            .fetch_one(&mut **self.tx)
            .await?;

        Ok(Todo {
            id: row.id,
            description: row.description,
        })
    }

    async fn get_all(&mut self) -> Result<Vec<Todo>, Box<dyn std::error::Error + Send + Sync>> {
        let todos: Vec<Todo> =
            sqlx::query_as!(Todo, r"SELECT id, description FROM todos ORDER BY id DESC")
                .fetch_all(&mut **self.tx)
                .await?;
        Ok(todos)
    }
}

#[async_trait]
impl<'a> RepoTrait<'a, User, UserRepo<'a>> for UserRepo<'a> {
    async fn new(
        tx: &'a mut Transaction<'static, sqlx::Postgres>,
    ) -> Result<UserRepo<'a>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(UserRepo { tx })
    }

    async fn insert(&mut self, user: User) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        query!(
            r#"INSERT INTO users (id, name) VALUES ( $1, $2 )"#,
            user.id,
            user.name,
        )
        // In 0.7, `Transaction` can no longer implement `Executor` directly,
        // so it must be dereferenced to the internal connection type.
        .execute(&mut **self.tx)
        .await?;

        println!("insert user(): {:?}", user);
        Ok(())
    }

    async fn get(&mut self, id: i64) -> Result<User, Box<dyn std::error::Error + Send + Sync>> {
        let row = query!(r#"SELECT id, name FROM users WHERE id = $1"#, id)
            .fetch_one(&mut **self.tx)
            .await?;

        Ok(User {
            id: row.id,
            name: row.name,
        })
    }

    async fn get_all(&mut self) -> Result<Vec<User>, Box<dyn std::error::Error + Send + Sync>> {
        let users: Vec<User> = sqlx::query_as!(User, r"SELECT id, name FROM users ORDER BY id")
            .fetch_all(&mut **self.tx)
            .await?;

        Ok(users)
    }
}
