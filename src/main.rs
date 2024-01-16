use async_trait::async_trait;
use sqlx::query;
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

    // test thread safe
    let mut handles = Vec::new();

    for y in 1..=300 {
        let pool = pool.clone();

        handles.push(task::spawn(async move {
            println!("after spawn : {}", y);
            let mut uow: sqlx::Transaction<'static, sqlx::Postgres> = pool.begin().await.unwrap();
            let mut todo_repo = TodoRepo::new(&mut uow).await.unwrap();

            println!("inside ythread : {}", y);
            for z in 1..=10 {
                let todo_id = z + (y - 1) * 10;
                todo_repo
                    .insert(Todo {
                        id: todo_id,
                        description: format!("test {} inside {}", todo_id, y),
                    })
                    .await
                    .unwrap();
            }

            match uow.commit().await {
                Ok(_) => println!("Transaction committed"),
                Err(e) => println!("Transaction error: {}", e),
            }
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
        print!("{},", todo.id);
    });

    Ok(())
}

#[derive(sqlx::FromRow, Debug)]
struct Todo {
    id: i64,
    description: String,
}

struct TodoRepo<'a> {
    tx: &'a mut sqlx::Transaction<'static, sqlx::Postgres>,
}

#[async_trait]
trait TodoRepoTrait<'a> {
    async fn new(
        tx: &'a mut sqlx::Transaction<'static, sqlx::Postgres>,
    ) -> Result<TodoRepo<'a>, Box<dyn std::error::Error + Send + Sync>>;
    async fn insert(&mut self, todo: Todo) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn get(&mut self, id: i64) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>;
    async fn get_all(&mut self) -> Result<Vec<Todo>, Box<dyn std::error::Error + Send + Sync>>;
}

#[async_trait]
impl<'a> TodoRepoTrait<'a> for TodoRepo<'a> {
    async fn new(
        tx: &'a mut sqlx::Transaction<'static, sqlx::Postgres>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
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

    async fn get(&mut self, id: i64) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        // check that inserted todo can be fetched inside the uncommitted transaction
        let rec = query!(r#"SELECT id FROM todos WHERE id = $1"#, id)
            .fetch_one(&mut **self.tx)
            .await?;

        Ok(rec.id)
    }

    async fn get_all(&mut self) -> Result<Vec<Todo>, Box<dyn std::error::Error + Send + Sync>> {
        // check that inserted todo can be fetched inside the uncommitted transaction
        let todos: Vec<Todo> =
            sqlx::query_as!(Todo, r"SELECT id, description FROM todos ORDER BY id DESC")
                .fetch_all(&mut **self.tx)
                .await?;

        Ok(todos)
    }
}
