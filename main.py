import requests
from dataclasses import dataclass


@dataclass()
class FileLoader:
    def __init__(self, path: str, store_path: str):
        self.path: str = path
        self.response = None
        self.store_path: str = store_path

    def __post_init__(self):
        self.response = requests.get(self.path)

    def download(self):
        response = requests.get(self.path)
        if response.status_code == 200:
            with open(self.store_path, "wb") as file:
                file.write(response.content)
            print(f"File downloaded successfully: {self.store_path}")
        else:
            print(f"Failed to download file. Status code: {response.status_code}")


if __name__ == "__main__":
    url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_gl.csv"
    store = "co2_mm_gl.csv"

    loader = FileLoader(url, store_path = store)
    loader.download()
