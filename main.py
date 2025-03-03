import argparse
import yaml
import subprocess
import os
import paramiko
import sys
import json
import time
import traceback
import requests
import datetime
import sqlite3
from prettytable import PrettyTable


class Config:
    def __init__(self, config):
        with open(config, 'r') as f:
            self.config = json.load(f)

    def hotkey(self):
        return self.config['hotkey']

    def miner_uid(self):
        return self.config['miner_uid']

    def primary_host(self):
        return self.config['primary_host']

    def chutes_audit_host(self):
        return self.config['chutes_audit']

class Sqlite:
    def __init__(self):
        self.db_name = "chutes_deployments.db"
        self.deployments_table = "deployments"
        self.records = []

    def init_db(self):
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS ''' + self.deployments_table + ''' (
                INSTANCE_ID CHAR(50) NOT NULL,
                DEPLOYMENT_ID CHAR(50) NOT NULL,
                CHUTE_ID CHAR(50) NOT NULL,
                HOST_IP CHAR(50) NOT NULL,
                GPU_TYPE CHAR(50) NOT NULL,
                CREATED_AT CHAR(50) NOT NULL,
                GPU_COUNT INT NOT NULL,
                DELETED_AT CHAR(50) DEFAULT 0
            );''')
        conn.commit()
        conn.close()

    def query_record(self, instance_id, deployment_id, chute_id, host_ip, gpu_type):
        records = []
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        c.execute('''SELECT * FROM ''' + self.deployments_table +
                ''' WHERE INSTANCE_ID = ? AND DEPLOYMENT_ID = ? AND CHUTE_ID = ? AND HOST_IP = ? AND GPU_TYPE = ? ;''', 
                (instance_id, deployment_id, chute_id, host_ip, gpu_type))
        for row in c:
            records.append(row)
        conn.close()
        return records


    def insert_into_db(self, instance_id, deployment_id, chute_id, host_ip, gpu_type, created_at, gpu_count):
        records = self.query_record(instance_id, deployment_id, chute_id, host_ip, gpu_type)
        if len(records) == 0:
            conn = sqlite3.connect(self.db_name)
            c = conn.cursor()
            c.execute('''INSERT OR IGNORE INTO ''' + self.deployments_table +
                    ''' (INSTANCE_ID, DEPLOYMENT_ID, CHUTE_ID, HOST_IP, GPU_TYPE, CREATED_AT, GPU_COUNT) VALUES (?, ?, ?, ?, ?, ?, ?);''',
                    (instance_id, deployment_id, chute_id, host_ip, gpu_type, created_at, gpu_count))
            conn.commit()
            conn.close()

    def update_deployment_deleted_at(self, deleted_at, instance_id):
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        c.execute('''UPDATE ''' + self.deployments_table +
                ''' SET DELETED_AT = ? WHERE INSTANCE_ID = ?;''',
                (deleted_at, instance_id))
        conn.commit()
        conn.close()

    def query_records(self):
        self.records = []
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        c.execute('''SELECT * FROM ''' + self.deployments_table + ''' WHERE DELETED_AT=0 ORDER BY CREATED_AT DESC;''')
        for row in c:
            self.records.append(row)
        conn.close()
        print(self.records)


class Executor:
    def __init__(self, config, sql):
        self.config = config
        self.sql = sql
        self.hotkey = self.config.hotkey()
        self.miner_uid = self.config.miner_uid()
        self.chutes_nodes = {}
        self.primary_host = self.config.primary_host()
        self.chutes_audit_host = self.config.chutes_audit_host()
        self.instance_infos = {}


    def execute_ssh_command(self, host, username, command):
        cli = paramiko.SSHClient()
        try:
            cli.set_missing_host_key_policy(paramiko.AutoAddPolicy)
            cli.connect(hostname=host, username=username)
            _command = f'{command}'
            print(f'  \033[92m{command}\033[0m')
            stdin, stdout, stderr = cli.exec_command(_command)
            return (stderr.read().decode('utf-8'), stdout.read().decode('utf-8'))
        finally:
            cli.close()

    def fetch_deployments_from_k8s(self):
        pod_name = self.primary_host['pod_name']
        command = f'microk8s kubectl exec -n {pod_name} -- psql -U chutes chutes -c "select instance_id,tmp.deployment_id,chute_id,host,model_short_ref,created_at,gpu_count from (select count(*) as gpu_count,deployment_id,model_short_ref from gpus group by deployment_id,model_short_ref) AS tmp JOIN deployments as d on tmp.deployment_id = d.deployment_id;" | grep \'-\' | grep -v \'\\-\\-\' '
        return self.execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)

    def insert_into_sqlite(self):
        (err, out) = self.fetch_deployments_from_k8s()
        if err != '':
            raise Exception(err)
        for deployment in out.splitlines():
            instance_id = deployment.split('|')[0].strip()
            deployment_id = deployment.split('|')[1].strip()
            chute_id = deployment.split('|')[2].strip()
            host_ip = deployment.split('|')[3].strip()
            gpu_type = deployment.split('|')[4].strip()
            created_at = deployment.split('|')[5].strip()
            gpu_count = deployment.split('|')[6].strip()
            self.sql.insert_into_db(instance_id, deployment_id, chute_id, host_ip, gpu_type, created_at, gpu_count)

    def fetch_audit_latest_time(self):
        pod_name = self.chutes_audit_host['pod_name']
        today = datetime.date.today()
        command = f'sudo docker exec {pod_name} psql -U user chutes_audit -c \"SELECT max(started_at) FROM invocations;\" | grep {today}'
        (err, out) = self.execute_ssh_command(self.chutes_audit_host['host_ip'], self.chutes_audit_host['username'], command)
        if err != "":
            raise Exception(err)
        latest_time = out.strip()
        self.latest_time = latest_time

    def fetch_instance_chute_compute_units(self, instance_id, latest_time, check_interval):
        pod_name = self.chutes_audit_host['pod_name']
        command = f'sudo docker exec {pod_name} psql -U user chutes_audit -c \"SELECT * FROM (SELECT i.miner_hotkey, SUM(i.compute_multiplier * EXTRACT(EPOCH FROM (i.completed_at - i.started_at))) AS compute_units, i.instance_id FROM invocations i WHERE i.started_at > \'{latest_time}\'::TIMESTAMP - INTERVAL \'{check_interval}\' AND i.error_message IS NULL AND miner_uid={self.miner_uid} AND instance_id=\'{instance_id}\' GROUP BY i.miner_hotkey, i.instance_id HAVING SUM(i.compute_multiplier * EXTRACT(EPOCH FROM (i.completed_at - i.started_at))) > 0 ORDER BY compute_units DESC) AS A JOIN (SELECT instance_id,compute_multiplier, SUM(EXTRACT(EPOCH FROM (completed_at - started_at))) from invocations WHERE started_at > \'{latest_time}\'::TIMESTAMP - INTERVAL \'{check_interval}\' AND error_message IS NULL group by instance_id,compute_multiplier ) as B on A.instance_id = B.instance_id;\" | grep {self.hotkey}'
        (err, out) = self.execute_ssh_command(self.chutes_audit_host['host_ip'], self.chutes_audit_host['username'], command)
        if err != '':
            raise Exception(err)
        if out == '':
            return(0, 0, 0)
        else:
            return(out.split('|')[1].strip(), out.split('|')[4].strip(), out.split('|')[5].strip())

    def fetch_instance_invocation_count(self, instance_id, latest_time, check_interval):
        pod_name = self.chutes_audit_host['pod_name']
        command = f'sudo docker exec {pod_name} psql -U user chutes_audit -c \"SELECT instance_id, count(*) FROM invocations WHERE started_at > \'{latest_time}\'::TIMESTAMP - INTERVAL \'{check_interval}\' AND error_message IS NULL AND miner_uid={self.miner_uid} AND instance_id=\'{instance_id}\' GROUP BY instance_id;\" | grep {instance_id}'
        (err, out) = self.execute_ssh_command(self.chutes_audit_host['host_ip'], self.chutes_audit_host['username'], command)
        if err != '':
            raise Exception(err)
        if out == '':
            return 0
        else:
            return out.split('|')[1].strip()

    def fetch_instance_deleted_at(self, instance_id):
        pod_name = self.primary_host['pod_name']
        command = f'microk8s kubectl exec -n {pod_name} -- psql -U chutes chutes -c "select instance_id, deleted_at from deployment_audit where instance_id = \'{instance_id}\';" | grep {instance_id}'
        (err, out) = self.execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)
        if err != '':
            raise Exception(err)
        if out == '':
            return 0
        else:
            return out.split('|')[1].strip() if len(out.split('|')[1].strip()) > 0 else 0

    def update_instance_deleted_at(self, deleted_at, instance_id):
        self.sql.update_deployment_deleted_at(deleted_at, instance_id)

    def check_host_ip_is_active(self, host_ip):
        pod_name = self.primary_host['pod_name']
        command = f'microk8s kubectl get nodes -o wide --show-labels | grep {host_ip}'
        (err, out) = self.execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)
        if err != '':
            raise Exception(err)
        if out == '':
            return False
        else:
            return True if len(out.strip()) > 0 else False

    def fetch_instances_chutes_compute_units(self):
        self.instances_chutes_compute_units = {}
        for record in self.sql.records:
            instance_id = record[0]
            if self.instances_chutes_compute_units.get(instance_id) is None:
                (compute_units_1_hour, multiplier, elapsed_1_hour) = self.fetch_instance_chute_compute_units(instance_id, self.latest_time, '1 hour')
                (compute_units_1_day, multiplier, elapsed_1_day) = self.fetch_instance_chute_compute_units(instance_id, self.latest_time, '1 day')
                (compute_units_7_day, multiplier, elapsed_7_days) = self.fetch_instance_chute_compute_units(instance_id, self.latest_time, '7 days')
                invocation_count_1_hour = self.fetch_instance_invocation_count(instance_id, self.latest_time, '1 hour')
                invocation_count_1_day = self.fetch_instance_invocation_count(instance_id, self.latest_time, '1 day')
                invocation_count_7_days = self.fetch_instance_invocation_count(instance_id, self.latest_time, '7 days')
                deleted_at = self.fetch_instance_deleted_at(instance_id)
                self.update_instance_deleted_at(deleted_at, instance_id)
                self.instances_chutes_compute_units[instance_id] = {
                  "compute_units_1_hour": compute_units_1_hour,
                  "compute_units_1_day": compute_units_1_day,
                  "compute_units_7_days": compute_units_7_day,
                  "multiplier": multiplier,
                  "elapsed_1_hour": elapsed_1_hour,
                  "elapsed_1_day": elapsed_1_day,
                  "elapsed_7_days": elapsed_7_days,
                  "invocation_count_1_hour": invocation_count_1_hour,
                  "invocation_count_1_day": invocation_count_1_day,
                  "invocation_count_7_days": invocation_count_7_days,
                  "deployment_id": record[1],
                  "chute_id": record[2],
                  "host_ip": record[3],
                  "model_short_ref": record[4],
                  "started_at": record[5],
                  "gpu_count": record[6],
                  "deleted_at": deleted_at
                }
        print(self.instances_chutes_compute_units)


    def print_hosts_compute_units(self):
        t = PrettyTable(['Host IP', 'Active', 'GPU Type', 'Compute Units 1 hour', 'Compute Units 1 day', 'Compute Units 7 days'])
        hosts_compute_units = {}
        for instance, compute_units in self.instances_chutes_compute_units.items():
            hosts_compute_units[compute_units['host_ip']] = {
                'model_short_ref': compute_units['model_short_ref'],
                'compute_units_1_hour': compute_units['compute_units_1_hour'] if compute_units['host_ip'] not in hosts_compute_units else float(hosts_compute_units[compute_units['host_ip']]['compute_units_1_hour']) + float(compute_units['compute_units_1_hour']),
                'compute_units_1_day': compute_units['compute_units_1_day'] if compute_units['host_ip'] not in hosts_compute_units else float(hosts_compute_units[compute_units['host_ip']]['compute_units_1_day']) + float(compute_units['compute_units_1_day']),
                'compute_units_7_days': compute_units['compute_units_7_days'] if compute_units['host_ip'] not in hosts_compute_units else float(hosts_compute_units[compute_units['host_ip']]['compute_units_7_days']) + float(compute_units['compute_units_7_days'])
            }

        for host_ip, compute_units in hosts_compute_units.items():
            t.add_row([
                host_ip,
                self.check_host_ip_is_active(host_ip),
                compute_units['model_short_ref'],
                compute_units['compute_units_1_hour'],
                compute_units['compute_units_1_day'],
                compute_units['compute_units_7_days']
                ])

        print(t.get_string(sortby="Active"))

    def print_hosts_chutes_compute_units(self):
        t = PrettyTable(['Host IP', 'GPU Type', 'GPU Count', 'Instace ID', 'Chute ID', 'Deployment ID', 'Running Time', 'Active', 'Units 1h', 'Units 1d', 'Units 7d', 'Invocations 1h', 'Invocations 1d', 'Invocations 7d', 'multiplier', 'Elapsed 1h', 'Elapsed 1d', 'Elapsed 7d'])
        hosts_compute_units = {}
        for instance, compute_units in self.instances_chutes_compute_units.items():
            t.add_row([
                compute_units['host_ip'],
                compute_units['model_short_ref'],
                compute_units['gpu_count'],
                instance[-12:],
                compute_units['chute_id'][-12:],
                compute_units['deployment_id'][-12:],
                str(datetime.timedelta(seconds = time.time() - time.mktime(time.strptime(compute_units['started_at'], "%Y-%m-%d %H:%M:%S.%f+00")))).split('.')[0],
                False if compute_units['deleted_at'] != 0 else True,
                str(compute_units['compute_units_1_hour']).split('.')[0],
                str(compute_units['compute_units_1_day']).split('.')[0],
                str(compute_units['compute_units_7_days']).split('.')[0],
                compute_units['invocation_count_1_hour'],
                compute_units['invocation_count_1_day'],
                compute_units['invocation_count_7_days'],
                compute_units['multiplier'],
                compute_units['elapsed_1_hour'],
                compute_units['elapsed_1_day'],
                compute_units['elapsed_7_days']
            ])
        print(t.get_string(sortby="Active"))


def main():
    parser = argparse.ArgumentParser(
        prog='chutes',
        description='chute deployment analyse',
        epilog='Copyright(r), 2025'
    )

    parser.add_argument('-c', '--config', required=True)

    args = parser.parse_args()

    try:
        config = Config(args.config)
    except Exception as e:
        print(f'\33[31mFailed load config: {e}\33[0m')

    sql = Sqlite()
    sql.init_db()

    executor = Executor(config, sql)
    executor.insert_into_sqlite()
    sql.query_records()

    executor.fetch_audit_latest_time()
    executor.fetch_instances_chutes_compute_units()
    executor.print_hosts_compute_units()
    executor.print_hosts_chutes_compute_units()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'\33[31mFailed run deployment: {e}\033[0m')
        traceback.print_exc()
