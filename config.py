import yaml
from jsonschema import validate

class Config:
    def __init__(self, conf):
        self.conf = conf

    @classmethod
    def load(self, path):
        with open(path) as file:
            conf = yaml.safe_load(file)
        return Config(conf)

    def valid(self):
        schema = yaml.safe_load(self.schema)
        return validate(self.conf, schema)

    @property
    def schema(self):
        return '''
type: object
properties:
  init:
    type: object
    required:
      - service
    properties:
      credential-file:
        type: string
      service:
        type: string
        enum:
          - bigquery
  in:
    type: object
    required:
      - query
    properties:
      query:
        type: string
      vars:
        type: array
        items:
          type: object
          required:
            - name
            - database
            - mode
            - field
          properties:
            name:
              type: string
            database:
              type: string
            mode:
              type: string
              enum:
                - meta
                - max
                - min
            field:
              type: string
            default:
              type: string
  out:
    type: object
    required:
      - project
      - database
      - table
      - mode
    properties:
      project:
        type: string
      database:
        type: string
      table:
        type: string
      mode:
        type: string
        enum:
          - append
          - replace
          - merge
      merge:
        type: object
        properties:
          order:
            type: array
            items:
              type: object
              required:
                - column
              properties:
                column:
                  type: string
                desc:
                  type: boolean
          keys:
            type: array
            items:
              type: string
        '''
