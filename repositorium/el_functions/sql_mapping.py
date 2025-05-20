# MS SQL Server → DuckDB Mapping
MSSQL_TO_DUCKDB_MAP = {
    -7: "BOOLEAN",       # BIT
    1: "VARCHAR(256)",
    2: "DATE",
    3: "INTEGER",        # INT 
    4: "DATETIME",
    5: "DECIMAL(38,20)", # DECIMAL
}