"""Downloader module - Downloads PaySim dataset from Kaggle."""

from pathlib import Path
import kaggle

from stream_service.utils import console
from stream_service import constants


def download_dataset() -> bool:
    """
    Download PaySim dataset from Kaggle if not already present.
    
    Returns:
        bool: True if dataset is ready (downloaded or already exists), False on error.
    """
    csv_file = Path(constants.CSV_PATH)
    
    if csv_file.exists():
        size_mb = csv_file.stat().st_size / (1024 * 1024)
        console.print(f"[green]Dataset already exists:[/green] {csv_file.name} ({size_mb:.1f} MB)")
        return True
    
    output_path = csv_file.parent
    output_path.mkdir(parents=True, exist_ok=True)
    
    console.print("[blue]Downloading PaySim dataset from Kaggle...[/blue]")
    
    try:
        kaggle.api.dataset_download_files("ealaxi/paysim1", path=str(output_path), unzip=True)
        console.print("[green]Download complete![/green]")
        return True
    except Exception as e:
        console.print(f"[red]Download failed:[/red] {e}")
        return False
