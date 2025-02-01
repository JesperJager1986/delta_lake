import requests
from dataclasses import dataclass
from pathlib import Path
import os

@dataclass()
class FileDownloader:
    def __init__(self, path: str, store_path: str | None = None):
        self.path: str = path
        self.response = None
        self.store_file = Path("download") / Path(path).name if store_path is None else Path(store_path)

    def __post_init__(self):
        self.create_folder()
        self.response= requests.get(self.path)

    def create_folder(self):
        os.makedirs(self.path, exist_ok=True)

    def download(self):
        response = requests.get(self.path)
        if response.status_code == 200:
            with open(self.store_file, "wb") as file:
                file.write(response.content)
            print(f"File downloaded successfully: {self.store_file}")
        else:
            print(f"Failed to download file. Status code: {response.status_code}")


if __name__ == "__main__":
    url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_gl.csv"

    loader = FileDownloader(url)