import argparse
import json5


class Config:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", help="Config file", required=True)
        parser.add_argument("-a", "--auto-delete", action="store_true", help="Delete deployment automatically")
        self.args = parser.parse_args()

        self.db_name = "chutes_deployments.db"

        with open(self.args.config, 'r') as f:
            self.config = json5.load(f)

    def hotkey(self):
        return self.config['hotkey']

    def miner_uid(self):
        return self.config['miner_uid']

    def primary_host(self):
        return self.config['primary_host']

    def chutes_audit_host(self):
        return self.config['chutes_audit']

    def reconcilation(self):
        return self.config['reconcilation']

    def auto_delete(self):
        return self.args.auto_delete

    def database_file(self):
        return self.db_name
