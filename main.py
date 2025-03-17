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

from config import Config
from remote_ssh import execute_ssh_command

from reconcilation import Reconcilation

from print_table import display_instance_metrics
from sqlite_op import SQLiteInstance
from prettytable import PrettyTable


class Executor:
    def __init__(self, config, instance_db):
        self.config = config
        self.instance_db = instance_db
        self.instance_db_records = []
        self.latest_instances = []
        self.hotkey = self.config.hotkey()
        self.miner_uid = self.config.miner_uid()
        self.instances_metrics = {}
        self.primary_host = self.config.primary_host()
        self.chutes_audit_host = self.config.chutes_audit_host()


    def fetch_deployments_from_primary(self):
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
        instances = self.instance_db.query_active_instances()
        self.instance_db_records.extend(instances)


    def query_model_from_primary(self, chute_id):
        pod_name = self.primary_host['pod_name']
        command = f'''
            microk8s kubectl exec -n {pod_name} -- psql -U chutes chutes -c \
            \"select code from chutes where chute_id = '{chute_id}';\" | grep -v "model_name_or_url\|username" | grep "model_name=\| name=" | awk -F \'\"\' \'{{ print $2 }}\'
        '''
        return execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)


    def update_chute_mode_name(self):
        for record in self.instance_db_records:
            chute_data = [record.chute_id]
            if self.instance_db.chute_model_exists(chute_data) is False:
                (err, out) = self.query_model_from_primary(record.chute_id)
                if err != '':
                    raise Exception(err)
                model_name = out.splitlines()[0]
                self.instance_db.insert_chute_model((record.chute_id, model_name))


    def update_instances_model_name(self):
        for instance, metric in self.instances_metrics.items():
            chute_data = [metric['chute_id']]
            model_name = self.instance_db.query_chute_model_name(chute_data)
            self.instances_metrics[instance]['model_name'] = model_name


    def fetch_latest_instances(self):
        (err, out) = self.fetch_deployments_from_primary()
        if err != '':
            raise Exception(err)
        for latest_instance in out.splitlines():
            latest_instances = tuple([x.strip() for x in latest_instance.strip().split('|')])
            if self.instance_db.instance_exists(latest_instances) is False:
                self.instance_db.insert_instance(latest_instances)


    def update_expired_instances(self):
        for instance_id in self.instances_metrics:
            deleted_at = self.instances_metrics[instance_id]['deleted_at']
            self.update_instance_deleted_at(deleted_at, instance_id)


    def query_latest_audit_time(self):
        pod_name = self.chutes_audit_host['pod_name']
        today = datetime.date.today()
        command = f'sudo docker exec {pod_name} psql -U user chutes_audit -c \"SELECT max(started_at) FROM invocations;\" | grep {today}'
        (err, out) = execute_ssh_command(self.chutes_audit_host['host_ip'], self.chutes_audit_host['username'], command)
        if err != "":
            raise Exception(err)
        self.latest_time = out.strip()


    def fetch_instance_performance_metrics(self, instance_id, latest_time, check_interval):
        metric = {
                'compute_units': 0,
                'bounty': 0,
                'invocation_count': 0
                }
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
                    AND NOT EXISTS (SELECT 1 FROM reports WHERE invocation_id = i.parent_invocation_id AND confirmed_at IS NOT NULL) \
                    GROUP BY i.miner_hotkey \
                    ORDER BY compute_units DESC;\" | grep {self.hotkey}'''
        (err, out) = execute_ssh_command(self.chutes_audit_host['host_ip'], self.chutes_audit_host['username'], command)
        if err != '':
            raise Exception(err)
        if out == '':
            return metric
        else:
            metric['compute_units'] = out.split('|')[1].strip()
            metric['bounty'] = out.split('|')[3].strip()
            metric['invocation_count'] = out.split('|')[4].strip()
            return metric


    def fetch_instance_end_time(self, deployment_id):
        pod_name = self.primary_host['pod_name']
        command = f'microk8s kubectl exec -n {pod_name} -- psql -U chutes chutes -c "select deployment_id, deleted_at from deployment_audit \
                where deployment_id = \'{deployment_id}\';" | grep {deployment_id} | awk -F "|" \'{{ print $2 }}\''
        (err, out) = execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)
        if err != '':
            raise Exception(err)
        else:
            return out.strip() if len(out.strip()) > 0 else 0


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


    def fetch_instance_metrics(self, record):
        if self.instances_metrics.get(record.instance_id) is None:
            metrics_1_hour = self.fetch_instance_performance_metrics(record.instance_id, self.latest_time, '1 hour')
            metrics_1_day = self.fetch_instance_performance_metrics(record.instance_id, self.latest_time, '1 day')
            metrics_7_days = self.fetch_instance_performance_metrics(record.instance_id, self.latest_time, '7 days')

            deleted_at = self.fetch_instance_end_time(record.deployment_id)

            self.instances_metrics[record.instance_id] = {
                "instance_id": record.instance_id,
                "1_hour": {
                    "compute_units": metrics_1_hour['compute_units'],
                    "bounty": metrics_1_hour['bounty'],
                    "invocation_count": metrics_1_hour['invocation_count']
                },
                "1_day": {
                    "compute_units": metrics_1_day['compute_units'],
                    "bounty": metrics_1_day['bounty'],
                    "invocation_count": metrics_1_day['invocation_count']
                },
                "7_days": {
                    "compute_units": metrics_7_days['compute_units'],
                    "bounty": metrics_7_days['bounty'],
                    "invocation_count": metrics_7_days['invocation_count']
                },
                "deployment_id": record.deployment_id,
                "chute_id": record.chute_id,
                "host_ip": record.host_ip,
                "gpu_type": record.gpu_type,
                "started_at": record.created_at,
                "gpu_count": record.gpu_count,
                "deleted_at": deleted_at
            }


    def fetch_instances_metrics(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self.fetch_instance_metrics, record): record for record in self.instance_db_records}
            concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

        return self.instances_metrics


    def print_instances_performance(self):
        print(self.instances_metrics)
        t = PrettyTable(['Host IP', 'Active', 'GPU Type', 'Compute Units 1 hour', 'Compute Units 1 day', 'Compute Units 7 days'])
        hosts_metrics = {}
        for instance, instance_metrics in self.instances_metrics.items():
            hosts_metrics[instance_metrics['host_ip']] = {
                'gpu_type': instance_metrics['gpu_type'],
                '1_hour': instance_metrics['1_hour']['compute_units'] if instance_metrics['host_ip'] not in hosts_metrics else float(hosts_metrics[instance_metrics['host_ip']]['1_hour']) + float(instance_metrics['1_hour']['compute_units']),
                '1_day': instance_metrics['1_day']['compute_units'] if instance_metrics['host_ip'] not in hosts_metrics else float(hosts_metrics[instance_metrics['host_ip']]['1_day']) + float(instance_metrics['1_day']['compute_units']),
                '7_days': instance_metrics['7_days']['compute_units'] if instance_metrics['host_ip'] not in hosts_metrics else float(hosts_metrics[instance_metrics['host_ip']]['7_days']) + float(instance_metrics['7_days']['compute_units'])
            }

        for host_ip, host_metrics in hosts_metrics.items():
            t.add_row([
                host_ip,
                self.check_host_ip_is_active(host_ip),
                host_metrics['gpu_type'],
                host_metrics['1_hour'],
                host_metrics['1_day'],
                host_metrics['7_days']
            ])

        print(t.get_string(sortby="Active"))


    def print_instances_detail_performance(self):
        display_instance_metrics(self.instances_metrics, "Active Instances", "Active")


def main():
    config = Config()

    instance_db = SQLiteInstance(config.database_file())
    instance_db.connect()
    instance_db.create_table()

    executor = Executor(config, instance_db)

    executor.fetch_latest_instances()
    executor.query_active_instances()
    executor.update_chute_mode_name()

    executor.query_latest_audit_time()
    metrics = executor.fetch_instances_metrics()

    executor.update_expired_instances()
    executor.update_instances_model_name()

    instance_db.close_connection()

    executor.print_instances_performance()
    executor.print_instances_detail_performance()

    primary_host = config.primary_host()

    reconcilation = Reconcilation(config.reconcilation(), metrics, primary_host, config.auto_delete())
    reconcilation.do()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'\33[31mFailed run deployment: {e}\033[0m')
        traceback.print_exc()
