import sqlite3
import src as root
from pathlib import Path
import os

NUMBER_VOTES = 3
BATCH_SIZE = 10
MINIMUM_SAMPLES = 100

path = Path(root.__file__)
path = os.path.join(Path(root.__file__).parents[2], 'a2xnotator_ui/doccano/app/db.sqlite3')

conn = sqlite3.connect(path)

def create_batch():
    pass

def assign_project():
   pass

def pull_data():
    pass