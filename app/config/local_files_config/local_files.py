from pathlib import Path
from dotenv import load_dotenv

from app.utils.formatted_date_util import formatted_datetime

PROJECT_ROOT = Path(__file__).parent.parent.parent

load_dotenv(PROJECT_ROOT / '.env')

GLOBAL_TERRORISM_CSV = PROJECT_ROOT / 'data' / 'globalterrorismdb_0718dist.csv'
SECONDARY_TERROR_CSV = PROJECT_ROOT / 'data' / 'RAND_Database_of_Worldwide_Terrorism_Incidents.csv'
# GLOBAL_TERRORISM_CSV = PROJECT_ROOT / 'data' / 'GLOBAL_TERRORISM_CSV.csv'
# SECONDARY_TERROR_CSV = PROJECT_ROOT / 'data' / 'SECONDARY_TERROR_CSV.csv'
MERGED_FILES = PROJECT_ROOT / 'data' / 'merged_files' / f'final-data-{formatted_datetime()}.csv'
NEO4J_QUERIES = PROJECT_ROOT / 'data' / f'neo4j-queries-{formatted_datetime()}.cypher'
