# Research Library — Assignment 3

## Install requierments

[LM Studio](https://lmstudio.ai/)
[AIRFLOW](https://airflow.apache.org/docs/apache-airflow/stable/start.html)

## Run the project

1. In **LM Studio**, load `google/gemma-4-e4b`, start the server on port
   **1234**, and enable **Serve on Local Network**.
2. In **Ubuntu/WSL**, copy the DAG into Airflow's monitored folder, then start
   Airflow:

   ```bash
   cd "[PATHS to project directory]"
   source .venv/bin/activate
   mkdir -p ~/airflow/dags
   cp dags/paper_pipeline.py ~/airflow/dags/
   airflow standalone
   ```

3. Open [Airflow](http://localhost:8080), enable `paper_pipeline`, and click
   **Trigger**. The DAG then runs every five minutes.
4. Place PDFs in `[Path to the drive folder]/papers`.
5. The pdf will be processed by airflow
6. Open `[Path to the drive folder]/output/index.html` to browse and search
   the papers. A Discord message is sent for each newly processed paper.

You can see the credentials using this command and you can change it using nano:

```bash
cat ~/airflow/simple_auth_manager_passwords.json.generated
```

## Install in an existing Airflow environment

```bash
cd "[PATHS to project directory]"
source .venv/bin/activate
uv pip install -r requirements-pipeline.txt
mkdir -p ~/airflow/dags
cp dags/paper_pipeline.py ~/airflow/dags/
```

**The file must be in `~/airflow/dags/` to appear in Airflow.** The project's
`dags/` folder and Airflow's DAG folder are separate directories. 

```bash
cd "[PATHS to project directory]"
cp dags/paper_pipeline.py ~/airflow/dags/
```

Airflow automatically detects the update.


```text
[Path to the drive folder]/
├── papers/
└── output/
    └── index.html
```

Set `LIBRARY_DIR` in `dags/paper_pipeline.py` to `[Path to the drive folder]`,
a Google Drive-synced folder, using its WSL path when running Airflow in WSL.
The DAG derives `papers/` and `output/` from this folder.

SQLite is stored locally in `~/airflow/data/paper.db`, outside Google Drive.
The database is created automatically on first database access.
When migrating an existing installation, back up the previous database
(`~/airflow/paper.db`, `~/airflow/data/papers.db` or `~/airflow/research_library.sqlite3`) to
`~/airflow/data/paper.db` before restarting the DAG to keep existing records.

Settings are defined at the top of `dags/paper_pipeline.py`: folders, LM Studio
address, and model name.
Modify these values before copying the DAG to another machine. Set the LM Studio URL to
`http://localhost:1234/v1` when LM Studio and Airflow share the same network
environment. 

Create a webhook in **Discord → Edit Channel → Integrations → Webhooks**,
copy its URL, and set `DISCORD_WEBHOOK_URL` at the top of
`dags/paper_pipeline.py`:

```python
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/...'
```



## Reset the project

Stop Airflow with **Ctrl+C** in its terminal, then run in Ubuntu/WSL:

```bash
cd "[PATHS to project directory]"
source .venv/bin/activate
airflow db reset
rm -i ~/airflow/data/paper.db
```
