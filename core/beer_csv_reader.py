from utils.custom_logger import *
from settings.settings import *
import csv

custom_logger = CustomLogger()

class BeerCsvReader:

    beers_data = []

    def __init__(self):
        custom_logger.log("BeerCsvReader {0}".format(CSV_FILE_PATH) )
        self._read_csv_file()

    def _read_csv_file(self):
        try:
            with open(CSV_FILE_PATH, "rb") as csvfile:
                beer_file = csv.reader(csvfile, delimiter = ",")
                next(beer_file, None)#skip headers
                self.beers_data = list(beer_file)
        except:
            custom_logger.log("Can't find csv file {0}".format(CSV_FILE_PATH))

    def get_beers_data(self):
        return self.beers_data
