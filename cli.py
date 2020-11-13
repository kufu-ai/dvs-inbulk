import argparse
import yaml
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

def main():
    conf = config.Config.load(args.path[0])
    conf.valid()
    b = bq.BigQuery(conf.conf)
    b.wait_job()

if __name__ == "__main__":
    main()
