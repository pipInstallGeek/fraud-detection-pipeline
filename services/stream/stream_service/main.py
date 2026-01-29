"""
Stream Service - Main Entrypoint

Orchestrates the complete startup sequence:
1. Wait for and register Debezium connectors
2. Download dataset if missing
3. Start continuous transaction streaming
"""

import sys

from stream_service.utils import console
from stream_service.connector import register_connectors
from stream_service.downloader import download_dataset
from stream_service.streamer import stream_transactions


def main() -> int:
    """Main entrypoint for the stream service."""
    console.print("\n[bold cyan]╔══════════════════════════════════════╗[/bold cyan]")
    console.print("[bold cyan]║   Fraud Detection Stream Service     ║[/bold cyan]")
    console.print("[bold cyan]╚══════════════════════════════════════╝[/bold cyan]\n")
    
    # Step 1: Register Debezium connectors
    console.print("[bold]Step 1:[/bold] Registering Debezium connectors...")
    if not register_connectors():
        console.print("[red]Failed to register connectors. Exiting.[/red]")
        return 1
    console.print()
    
    # Step 2: Download dataset if missing
    console.print("[bold]Step 2:[/bold] Checking dataset...")
    if not download_dataset():
        console.print("[red]Failed to download dataset. Exiting.[/red]")
        return 1
    console.print()
    
    # Step 3: Start streaming
    console.print("[bold]Step 3:[/bold] Starting transaction stream...")
    stream_transactions()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
