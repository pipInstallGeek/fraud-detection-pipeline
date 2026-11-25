"""Streamer module - Streams transactions from CSV to PostgreSQL."""

import time
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from stream_service.utils import console
from stream_service import constants
import stream_service.utils as utils


def stream_transactions() -> None:
    """
    Stream transactions from CSV file to PostgreSQL.
    
    This function reads the CSV in chunks and inserts them into the transactions table
    at a controlled rate. It runs until the CSV is exhausted or the process is interrupted.
    """
    csv_path = constants.CSV_PATH
    batch_size = constants.BATCH_SIZE
    rows_per_sec = constants.ROWS_PER_SECOND
    
    console.print("\n[bold blue]=== Transaction Stream Started ===[/bold blue]\n")
    console.print(f"CSV: {csv_path}")
    console.print(f"Batch: {batch_size} | Rate: {rows_per_sec}/sec\n")
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=constants.POSTGRES_HOST,
            port=constants.POSTGRES_PORT,
            dbname=constants.POSTGRES_DB,
            user=constants.POSTGRES_USER,
            password=constants.POSTGRES_PASSWORD,
        )
        conn.autocommit = True
        cursor = conn.cursor()
        console.print("[green]Connected to PostgreSQL[/green]\n")
    except Exception as e:
        console.print(f"[red]PostgreSQL connection failed:[/red] {e}")
        return
    
    # Check how many transactions already exist in PostgreSQL
    cursor.execute("SELECT COUNT(*) FROM transactions;")
    existing_count = cursor.fetchone()[0]
    if existing_count > 0:
        console.print(f"[yellow]Resuming stream: skipping first {existing_count:,} already-inserted rows from CSV.[/yellow]\n")
        skip_rows = range(1, existing_count + 1)
    else:
        skip_rows = None

    columns = [
        'step', 'type', 'amount', 'name_orig', 'old_balance_orig',
        'new_balance_orig', 'name_dest', 'old_balance_dest',
        'new_balance_dest', 'is_fraud', 'is_flagged_fraud'
    ]
    
    sleep_time = batch_size / rows_per_sec
    total_rows = 0
    batch_num = 0
    start_time = time.time()
    
    try:
        csv_iterator = pd.read_csv(
            csv_path,
            chunksize=batch_size,
            skiprows=skip_rows,
        )
        for chunk in csv_iterator:
            if not utils.running:
                break
            
            chunk.columns = columns
            values = [tuple(row) for row in chunk.values]
            
            query = f"INSERT INTO transactions ({', '.join(columns)}) VALUES %s"
            execute_values(cursor, query, values)
            
            total_rows += len(chunk)
            batch_num += 1
            elapsed = time.time() - start_time
            rate = total_rows / elapsed if elapsed > 0 else 0
            
            console.print(f"[{batch_num}] +{len(chunk)} rows | Session: {total_rows:,} | Total DB: {existing_count + total_rows:,} | {rate:.1f}/sec")
            
            time.sleep(sleep_time)
    
    except Exception as e:
        console.print(f"[red]Streaming error:[/red] {e}")
    finally:
        cursor.close()
        conn.close()
    
    elapsed = time.time() - start_time
    console.print(f"\n[bold green]Stream complete![/bold green] {total_rows:,} rows in {elapsed:.0f}s\n")
