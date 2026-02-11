# ETL Pipeline

Pipeline ETL orquestrado com Dagster para processamento de dados de energia eólica.

## Como Rodar

### Pré-requisitos
*   Docker e Docker Compose instalados.

### Execução

1.  **Configure as variáveis de ambiente:**
    ```bash
    cp .env.example .env
    ```

2.  **Suba o ambiente:**
    ```bash
    docker-compose up -d --build
    ```

3.  **Popule o banco de dados fonte:**
    ```bash
    docker-compose exec dagster_webserver  python scripts/init_source_db.py
    ```

4.  **Crie as tabelas no banco de dados alvo:**
    ```bash
    docker-compose exec dagster_webserver  python scripts/init_target_db.py
    ```

### ✅ Acesso

*   **Dagster UI:** [http://localhost:3000](http://localhost:3000)
*   **API:** [http://localhost:8000/docs](http://localhost:8000/docs)