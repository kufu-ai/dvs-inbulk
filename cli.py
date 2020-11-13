import argparse
import yaml
import bq

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
    with open(args.path[0]) as file:
        conf = yaml.safe_load(file)
    b = bq.BigQuery(conf)
    b.wait_job()

if __name__ == "__main__":
    main()
