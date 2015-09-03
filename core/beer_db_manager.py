import psycopg2
from settings.db_settings import *
from utils.custom_logger import *

custom_logger = CustomLogger()

"""
0 => number
1 => name
2 => description
3 => www
4 => picture_path
5 => address_name
6 => address_lat
7 => address_lon
8 => city_name
9 => province_name
"""

class BeerDbManager:

    def __init__(self, beers_data):
        self.beers_data = beers_data
        custom_logger.log("DB process {0} records".format(len(self.beers_data)))

    def _get_db_connection(self):
        try:
            return psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
        except Exception as e:
            return str(e)

    def proccess_data(self):
        conn = self._get_db_connection()
        if not type(conn) == str:
            for row in self.beers_data:
                self._proccess_row(conn, row)
                #break
            conn.close()
        else:
            custom_logger.log(conn)

    def _proccess_row(self, conn, row):
        if self._brewery_exists(conn, row[1]):
            custom_logger.log("Brewery exists {0}".format(row[1]))
            return
        province_id = self._get_province_id(conn, row[9])
        if province_id == None:
            custom_logger.log("Can't find province for '{0}'".format(row[9]))
            return
        city_id = self._add_city(conn, row[8], province_id)
        address_id = self._add_address(conn, city_id, row[5], row[6], row[7])
        self._add_brewery(conn, address_id, row[1], row[2], row[3], row[4])
        custom_logger.log("Added {0}".format(row[1]))

    def _brewery_exists(self, conn, brewery_name):
        cursor = conn.cursor()
        brewery_exists_select = "SELECT 1 FROM brewery WHERE brewery_name = %s"
        cursor.execute(brewery_exists_select,(brewery_name,))
        records = cursor.fetchone()
        return False if records == None else True

    def _add_brewery(self, conn, address_id, brewery_name, brewery_description, www, picture_path):
        cursor = conn.cursor()
        brewery_insert = """
                         INSERT INTO brewery(address_id,brewery_name,brewery_description,picture_path,www)
                         VALUES (%s,%s,%s,%s,%s)
                         """
        cursor.execute(brewery_insert,(address_id, brewery_name, brewery_description, picture_path, www))
        conn.commit()

    def _add_address(self, conn, city_id, address_name, lat, lon):
        cursor = conn.cursor()
        address_insert = "INSERT INTO address(city_id,address_name, lat,lon) VALUES (%s,%s,%s,%s) RETURNING address_id"
        address_id_select = "SELECT address_id FROM address WHERE lat = %s AND lon = %s"
        try:
            cursor.execute(address_insert,(city_id, address_name, lat, lon))
    	    records = cursor.fetchone()
            conn.commit()
            return records[0]
        except:
            conn.rollback()
            cursor.execute(address_id_select,(lat,lon))
            records = cursor.fetchone()
            return records[0]

    def _add_city(self, conn, city_name, province_id):
        cursor = conn.cursor()
        city_insert = "INSERT INTO city(city_name,province_id) VALUES (%s,%s) RETURNING city_id"
        city_id_select = "SELECT city_id FROM city WHERE city_name = %s AND province_id = %s"
        try:
            cursor.execute(city_insert,(city_name,province_id))
    	    records = cursor.fetchone()
            conn.commit()
            return records[0]
        except:
            conn.rollback()
            cursor.execute(city_id_select,(city_name,province_id))
            records = cursor.fetchone()
            return records[0]

    def _get_province_id(self,conn, province_name):
        cursor = conn.cursor()
        province_cmd = "SELECT province_id FROM province WHERE province_name = %s"
    	cursor.execute(province_cmd,(province_name,))
    	records = cursor.fetchone()
        if records:
            return records[0]
        else:
            return None
