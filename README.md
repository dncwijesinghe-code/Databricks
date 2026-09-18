# Databricks + Claude in VS Code

This starter connects to the Databricks workspace at:

`https://<your-workspace>.cloud.databricks.com`

It provides a small Python SQL client for data engineering and analytics work. Claude can work alongside it through the Claude Code VS Code extension and Databricks MCP, once that extension is installed and authenticated.

## One-time setup

Open a PowerShell terminal in this folder and run:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Fill in `.env` using the SQL warehouse's connection details in Databricks. `DATABRICKS_HTTP_PATH` normally looks like `/sql/1.0/warehouses/<warehouse-id>`. Store the token only in `.env`; `.gitignore` excludes it.

## Check the connection

```powershell
python .\databricks_query.py
python .\databricks_query.py "SELECT * FROM catalog.schema.table LIMIT 10"
```

The first command checks the authenticated user and catalog. The second runs an explicit query supplied on the command line.

## Claude in VS Code

Install the official Claude Code extension for VS Code, sign in, and open this folder. In Claude Code, add Databricks' managed SQL MCP server for this workspace using the Databricks MCP documentation and the same workspace host. Authenticate through the supported Databricks OAuth or CLI flow; do not paste a token into chat or commit an MCP configuration containing one.

For local Python work, select `.venv` as the VS Code Python interpreter. Claude can then inspect and edit the scripts, while Databricks MCP provides natural-language access to SQL metadata and queries.

## Databricks CLI authentication

For CLI-based jobs and bundles, authenticate without putting credentials in files:

```powershell
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks auth profiles
```

Use a workspace user with only the permissions needed for the warehouses, catalogs, schemas, and jobs you intend to access.
