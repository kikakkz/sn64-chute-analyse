import argparse
import json5


class Config:
    def __init__(self, config):
        with open(config, 'r') as f:
            self.config = json5.load(f)

    def hotkey(self):
        return self.config['hotkey']

    def miner_uid(self):
        return self.config['miner_uid']

    def primary_host(self):
        return self.config['primary_host']

    def chutes_audit_host(self):
        return self.config['chutes_audit']

    def fetch_delete_cfg(self):
        return self.config['delete_cfg']


def get_cli_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="config which contains your miner hotkey, miner uid and machine info", required=True)
    parser.add_argument("-a", "--auto-delete", action="store_true", help="Delete deployment automatically")
    args = parser.parse_args()
    return args
