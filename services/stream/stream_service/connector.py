"""Connector module - Registers Debezium connectors from JSON config."""

import json
import os
import time
import requests
from pathlib import Path

from stream_service.utils import console
from stream_service import constants


def _substitute_env_vars(config: dict) -> dict:
    """Replace ${VAR} placeholders in config with environment variable values."""
    result = {}
    for key, value in config.items():
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            result[key] = os.getenv(env_var, value)
        elif isinstance(value, dict):
            result[key] = _substitute_env_vars(value)
        else:
            result[key] = value
    return result


def _wait_for_debezium(url: str, max_retries: int = 30, retry_interval: int = 2) -> bool:
    """Wait for Debezium Connect to be available."""
    console.print(f"[blue]Waiting for Debezium at {url}...[/blue]")
    
    for i in range(max_retries):
        try:
            response = requests.get(f"{url}/connectors", timeout=5)
            if response.status_code == 200:
                console.print("[green]Debezium is ready![/green]")
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(retry_interval)
    
    console.print("[red]Debezium not available after retries[/red]")
    return False


def _connector_exists(url: str, name: str) -> bool:
    """Check if a connector already exists."""
    try:
        response = requests.get(f"{url}/connectors/{name}", timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def register_connectors() -> bool:
    """
    Register all connectors defined in connectors.json.
    
    Returns:
        bool: True if all connectors registered successfully, False otherwise.
    """
    url = constants.DEBEZIUM_URL
    config_path = Path(constants.CONNECTORS_CONFIG_PATH)
    
    # Wait for Debezium
    if not _wait_for_debezium(url):
        return False
    
    # Load connector configs
    if not config_path.exists():
        console.print(f"[red]Connector config not found:[/red] {config_path}")
        return False
    
    try:
        with open(config_path) as f:
            connectors = json.load(f)
    except json.JSONDecodeError as e:
        console.print(f"[red]Invalid JSON in connector config:[/red] {e}")
        return False
    
    console.print(f"[blue]Found {len(connectors)} connector(s) to register[/blue]")
    
    # Register each connector
    for connector in connectors:
        name = connector.get("name")
        if not name:
            console.print("[yellow]Skipping connector without name[/yellow]")
            continue
        
        # Check if already exists
        if _connector_exists(url, name):
            console.print(f"[green]Connector '{name}' already exists, skipping[/green]")
            continue
        
        # Substitute environment variables
        resolved_config = _substitute_env_vars(connector)
        
        # Register
        try:
            response = requests.post(
                f"{url}/connectors",
                json=resolved_config,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code in [200, 201]:
                console.print(f"[green]Registered connector:[/green] {name}")
            else:
                console.print(f"[red]Failed to register '{name}':[/red] {response.status_code} - {response.text}")
                return False
        except requests.exceptions.RequestException as e:
            console.print(f"[red]Error registering '{name}':[/red] {e}")
            return False
    
    return True
