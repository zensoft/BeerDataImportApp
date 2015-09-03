from utils.custom_logger import *
from core.beer_csv_reader import BeerCsvReader
from beer_db_manager import BeerDbManager
import os, time

custom_logger = CustomLogger()

"""
Main method that run logic
"""
def run_logic():
    beerCsvReader = BeerCsvReader()
    beers_data = beerCsvReader.get_beers_data()
    beerDbManager = BeerDbManager(beers_data)
    beerDbManager.proccess_data()
