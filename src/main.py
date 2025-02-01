
from AI_utility.src.download.FileDownLoader import FileDownloader

if __name__ == '__main__':
    url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_gl.csv"

    loader = FileDownloader(url)

    print(2)