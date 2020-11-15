import argparse
from logger import getLogger
import bq
import config

parser = argparse.ArgumentParser(description='ETL framework.')
parser.add_argument(
    'path',
    metavar='path',
    type=str,
    nargs='+',
    help='job yaml file path'
)
args = parser.parse_args()

log = getLogger(__name__)

def main():
    log.info('Job is starged')
    conf = config.Config.load(args.path[0])
    conf.valid()
    b = bq.BigQuery(conf.conf)
    b.wait_job()
    log.info('SUCCESS')

if __name__ == "__main__":
    main()
