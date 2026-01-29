"""Stream Service - Shared utilities."""

import signal
from rich.console import Console

console = Console()
running = True


def signal_handler(sig, frame):
    """Handle graceful shutdown on SIGINT/SIGTERM."""
    global running
    console.print("\n[yellow]Shutting down gracefully...[/yellow]")
    running = False


# Setup signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
