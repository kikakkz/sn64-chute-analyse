import yaml
import subprocess
import os
import sys
import json
import json5
import time
import traceback
import requests
import datetime
import uuid
import threading
import concurrent.futures

from get_args import get_cli_args
from get_args import Config
from remote_ssh import execute_ssh_command

from delete_instance import Deletion

from print_table import display_instance_chutes
from sqlite_op import *
from prettytable import PrettyTable

mutex = threading.Lock()


class Executor:
    def __init__(self, config, instance_db):
        self.config = config
        self.instance_db = instance_db
        self.records = []
        self.hotkey = self.config.hotkey()
        self.miner_uid = self.config.miner_uid()
        self.chutes_nodes = {}
        self.primary_host = self.config.primary_host()
        self.chutes_audit_host = self.config.chutes_audit_host()
        self.instance_infos = {}
        self.compute_units = {}


    def fetch_deployments_from_k8s(self):
        pod_name = self.primary_host['pod_name']
        command = f'''
            microk8s kubectl exec -n {pod_name} -- psql -U chutes chutes -c \
            \"select instance_id, tmp.deployment_id, chute_id, host, model_short_ref, created_at, gpu_count from \
            (select count(*) as gpu_count, deployment_id, model_short_ref from gpus group by deployment_id, model_short_ref) AS tmp \
            JOIN deployments as d \
            on tmp.deployment_id = d.deployment_id;\" | grep \'-\' | grep -v \'\\-\\-\'
        '''
        return execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)


    def query_active_instances(self):
        self.records = []
        instances = self.instance_db.query_active_instances()
        self.records.extend(instances)


    def insert_instances(self):
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

            if self.instance_db.instance_exists((instance_id, deployment_id, chute_id, host_ip, gpu_type)) is False:
                self.instance_db.insert_instance((instance_id, deployment_id, chute_id, host_ip, gpu_type, created_at, gpu_count))


    def query_latest_audit_time(self):
        pod_name = self.chutes_audit_host['pod_name']
        today = datetime.date.today()
        command = f'sudo docker exec {pod_name} psql -U user chutes_audit -c \"SELECT max(started_at) FROM invocations;\" | grep {today}'
        (err, out) = execute_ssh_command(self.chutes_audit_host['host_ip'], self.chutes_audit_host['username'], command)
        if err != "":
            raise Exception(err)
        latest_time = out.strip()
        self.latest_time = latest_time


    def fetch_instance_metrics(self, instance_id, latest_time, check_interval):
        pod_name = self.chutes_audit_host['pod_name']
        prefix = str(uuid.uuid1())[-12:]
        command = f'''sudo docker exec {pod_name} psql -U user chutes_audit -c \
            \"WITH {prefix}_computation_rates AS (SELECT chute_id, percentile_cont(0.5) WITHIN GROUP \
            (ORDER BY extract(epoch from completed_at - started_at) / (metrics->>\'steps\')::float) as median_step_time, \
            percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch from completed_at - started_at) / ((metrics->>\'it\')::float + (metrics->>\'ot\')::float)) as median_token_time \
            FROM invocations \
            WHERE ((metrics->>\'steps\' IS NOT NULL and (metrics->>\'steps\')::float > 0) \
                OR (metrics->>\'it\' IS NOT NULL AND metrics->>\'ot\' IS NOT NULL \
                AND (metrics->>\'ot\')::float > 0 \
                AND (metrics->>\'it\')::float > 0)) \
                AND started_at >= \'{latest_time}\'::TIMESTAMP - INTERVAL \'{check_interval}\' \
                AND miner_uid = {self.miner_uid} \
                AND instance_id = \'{instance_id}\' \
                GROUP BY chute_id) \
            SELECT i.miner_hotkey, COUNT(*) as invocation_count, COUNT(DISTINCT(i.chute_id)) AS unique_chute_count, \
                COUNT(CASE WHEN i.bounty > 0 THEN 1 END) AS bounty_count, \
                sum( i.bounty + i.compute_multiplier * CASE WHEN i.metrics->>\'steps\' IS NOT NULL \
                AND r.median_step_time IS NOT NULL THEN (i.metrics->>\'steps\')::float * r.median_step_time WHEN i.metrics->>\'it\' IS NOT NULL \
                AND i.metrics->>\'ot\' IS NOT NULL AND r.median_token_time IS NOT NULL \
                    THEN ((i.metrics->>\'it\')::float + (i.metrics->>\'ot\')::float) * r.median_token_time \
                    ELSE EXTRACT(EPOCH FROM (i.completed_at - i.started_at)) END ) AS compute_units  FROM invocations i \
                LEFT JOIN {prefix}_computation_rates r \
                    ON i.chute_id = r.chute_id \
                    WHERE i.started_at > \'{latest_time}\'::TIMESTAMP - INTERVAL \'{check_interval}\' \
                    AND i.error_message IS NULL \
                    AND i.miner_uid = {self.miner_uid} \
                    AND i.instance_id = \'{instance_id}\' \
                    AND i.completed_at IS NOT NULL \
                    GROUP BY i.miner_hotkey \
                    ORDER BY compute_units DESC;\" | grep {self.hotkey}'''
        (err, out) = execute_ssh_command(self.chutes_audit_host['host_ip'], self.chutes_audit_host['username'], command)
        if err != '':
            raise Exception(err)
        if out == '':
            return(0, 0, 0)
        else:
            return(out.split('|')[1].strip(), out.split('|')[3].strip(), out.split('|')[4].strip())

    def fetch_instance_invocation_count(self, instance_id, latest_time, check_interval):
        pod_name = self.chutes_audit_host['pod_name']
        command = f'sudo docker exec {pod_name} psql -U user chutes_audit -c \"SELECT instance_id, count(*) FROM invocations WHERE started_at > \'{latest_time}\'::TIMESTAMP - INTERVAL \'{check_interval}\' AND error_message IS NULL AND miner_uid={self.miner_uid} AND instance_id=\'{instance_id}\' GROUP BY instance_id;\" | grep {instance_id}'
        (err, out) = execute_ssh_command(self.chutes_audit_host['host_ip'], self.chutes_audit_host['username'], command)
        if err != '':
            raise Exception(err)
        if out == '':
            return 0
        else:
            return out.split('|')[1].strip()


    def fetch_instance_deleted_at(self, deployment_id):
        pod_name = self.primary_host['pod_name']
        command = f'microk8s kubectl exec -n {pod_name} -- psql -U chutes chutes -c "select deployment_id, deleted_at from deployment_audit where deployment_id = \'{deployment_id}\';" | grep {deployment_id}'
        (err, out) = execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)
        if err != '':
            raise Exception(err)
        if out == '':
            return 0
        else:
            return out.split('|')[1].strip() if len(out.split('|')[1].strip()) > 4 else 0


    def update_instance_deleted_at(self, deleted_at, instance_id):
        self.instance_db.update_instance_deleted_at((deleted_at, instance_id))


    def check_host_ip_is_active(self, host_ip):
        pod_name = self.primary_host['pod_name']
        command = f'microk8s kubectl get nodes -o wide --show-labels | grep {host_ip}'
        (err, out) = execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)
        if err != '':
            raise Exception(err)
        if out == '':
            return False
        else:
            return True if len(out.strip()) > 0 else False


    def fetch_instance_chutes_compute_units(self, record):
        instance_id = record[0]
        if self.instances_chutes_compute_units.get(instance_id) is None:
            metrics_1_hour = self.fetch_instance_compute(instance_id, self.latest_time, '1 hour')
            (invocation_count_1_day, bounty_count_1_day, compute_units_1_day) = self.fetch_instance_compute(instance_id, self.latest_time, '1 day')
            (invocation_count_7_days, bounty_count_7_days, compute_units_7_days) = self.fetch_instance_compute(instance_id, self.latest_time, '7 days')

            deleted_at = self.fetch_instance_deleted_at(record[1])
            mutex.acquire()
            try:
                self.instance_db.connect()
                self.update_instance_deleted_at(deleted_at, instance_id)
                time.sleep(1)
            except Exception as e:
                print(e)
            finally:
                mutex.release()
                self.instance_db.close_connection()

            self.instances_chutes_compute_units[instance_id] = {
                "instance_id": instance_id,
                "1_hour": {
                    "compute_units":,
                    "bounty":,
                },
                "compute_units": {
                    "1_hour": ,
                },
                "compute_units_1_hour": compute_units_1_hour,
                "compute_units_1_day": compute_units_1_day,
                "compute_units_7_days": compute_units_7_days,
                "bounty_count_1_hour": bounty_count_1_hour,
                "bounty_count_1_day": bounty_count_1_day,
                "bounty_count_7_days": bounty_count_7_days,
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


    def fetch_instances_metrics(self):
        self.instances_chutes_compute_units = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self.fetch_instance_chutes_compute_units, record): record for record in self.records}
            concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

        return self.instances_chutes_compute_units


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
                compute_units['gpu_short_ref'],
                compute_units['1_hour'],
                compute_units['1_day'],
                compute_units['7_days']
            ])

        print(t.get_string(sortby="Active"))


    def print_hosts_chutes_compute_units(self):
        display_instance_chutes(self.instances_chutes_compute_units, "Active Instances", "Active")


def main():
    config = Config()

    instance_db = SQLiteInstance(config.database_file())
    instance_db.connect()
    instance_db.create_table()

    executor = Executor(config, instance_db)

    executor.insert_instances()
    executor.query_active_instances()
    instance_db.close_connection()

    executor.query_latest_audit_time()
    metrics = executor.fetch_instances_metrics()

    executor.print_hosts_compute_units()
    executor.print_hosts_chutes_compute_units()

    primary_host = config.primary_host()

    reconcilation = Reconcilation(config.reconcilation(), metrics, primary_host)
    reconcilation.do()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'\33[31mFailed run deployment: {e}\033[0m')
        traceback.print_exc()
