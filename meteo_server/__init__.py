import pymysql
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "meteo_server.settings")
pymysql.install_as_MySQLdb()