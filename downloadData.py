import urllib.request
urllib.request.urlretrieve('https://archive.org/download/stackexchange/german.stackexchange.com.7z', './data/GermanStackExchange/GermanStackExchange.7z')
from pyunpack import Archive
Archive('./data/GermanStackExchange/GermanStackExchange.7z').extractall("./data/GermanStackExchange/")