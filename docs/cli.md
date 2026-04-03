# CLI (`ndb`)

`ndb` is NodeDB's native terminal client. It connects via the binary MessagePack protocol and provides a full TUI with syntax highlighting, tab completion, and multiple output formats.

## Connecting

`ndb` connects to NodeDB's **native MessagePack protocol** (default port 6433).

```bash
# Default (127.0.0.1:6433)
ndb

# Specify host and port
ndb -h db.example.com -p 6433

# With authentication
ndb -u admin -w                    # Prompt for password
ndb -u admin --password mypass     # Inline (not recommended)

# With TLS
ndb --tls --tls-ca-cert /path/to/ca.pem

# Execute a query and exit
ndb -e "SELECT * FROM users LIMIT 10"

# Execute from file
ndb -f queries.sql

# Write output to file
ndb -o results.json --format json -e "SELECT * FROM users"

# Pipe input
echo "SELECT 1" | ndb
```

## CLI Flags

| Flag | Long            | Default     | Purpose                                        |
| ---- | --------------- | ----------- | ---------------------------------------------- |
| `-h` | `--host`        | `127.0.0.1` | Server host                                    |
| `-p` | `--port`        | `6433`      | Server port (native protocol)                  |
| `-u` | `--user`        | `admin`     | Username                                       |
| `-w` | `--password`    |             | Password (prompts if flag given without value) |
| `-e` | `--execute`     |             | Execute SQL and exit                           |
| `-f` | `--file`        |             | Execute SQL from file and exit                 |
| `-o` | `--output`      |             | Write output to file                           |
|      | `--tls`         | off         | Enable TLS encryption                          |
|      | `--tls-ca-cert` |             | Path to CA certificate (PEM)                   |
|      | `--format`      | `table`     | Output format (`table`, `json`, `csv`)         |
|      | `--help`        |             | Print help information                         |

## Environment Variables

| Variable          | Purpose     | Precedence                  |
| ----------------- | ----------- | --------------------------- |
| `NODEDB_HOST`     | Server host | env > config file > default |
| `NODEDB_PORT`     | Server port | env > config file > default |
| `NODEDB_USER`     | Username    | env > config file > default |
| `NODEDB_PASSWORD` | Password    | env > config file > default |

CLI flags always take highest precedence.

## Interactive Features

- **Syntax highlighting** — Keywords, strings, numbers, and comments are color-coded
- **Tab completion** — SQL keywords, collection names, and column names (context-aware after FROM)
- **History search** — Ctrl+R for reverse search through command history
- **Persistent history** — Commands saved across sessions
- **Multi-line editing** — Write complex queries naturally
- **External editor** — `\e` opens your `$VISUAL` / `$EDITOR` for editing the current query

## Output Formats

```sql
-- Table format (default)
SELECT * FROM users;

-- JSON (NDJSON)
\format json
SELECT * FROM users;

-- CSV
\format csv
SELECT * FROM users;

-- Expanded/vertical (\x in psql)
\x
SELECT * FROM users;
```

## Useful Commands

| Command         | What it does                         |
| --------------- | ------------------------------------ |
| `\d`            | List collections (SHOW COLLECTIONS)  |
| `\d <name>`     | Describe a collection (DESCRIBE)     |
| `\x`            | Toggle expanded display              |
| `\e`            | Open external editor                 |
| `\g <file>`     | Write output to file                 |
| `\watch N`      | Re-run query every N seconds         |
| `\conninfo`     | Show connection details              |
| `\format <fmt>` | Set output format (table, json, csv) |

## Configuration

Config file at `~/.config/nodedb/config.toml`:

```toml
[connection]
host = "localhost"
port = 6433

[display]
format = "table"      # table, json, csv
expanded = false
```

[Back to docs](README.md)
