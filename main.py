import argparse
import yaml
import subprocess
import os
import paramiko
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

from prettytable import PrettyTable
from sqlite_op import *

mutex = threading.Lock()

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
        self.delete_cfg = {
            "running_time_for_hour": 7200,
            "compute_units_for_hour": 0,
            "invocation_count_for_hour": 0,
            "running_time_for_day": 86400,
            "compute_units_for_day": 0,
            "invocation_count_for_day": 0,
            "running_time_for_7day": 604800,
            "compute_units_for_7day": 0,
            "invocation_count_for_7day": 0,
            "local_chute_reserved": 0,
            "remote_chute_count": 0
        }
        for config, value in self.config['delete_cfg'].items():
            self.delete_cfg[config] = value if value >= self.delete_cfg[config] else self.delete_cfg[config]

        return self.delete_cfg


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
        self.delete_cfg = self.config.fetch_delete_cfg()


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
        command = f'''
            microk8s kubectl exec -n {pod_name} -- psql -U chutes chutes -c \
            \"select instance_id, tmp.deployment_id, chute_id, host, model_short_ref, created_at, gpu_count from \
            (select count(*) as gpu_count, deployment_id, model_short_ref from gpus group by deployment_id, model_short_ref) AS tmp \
            JOIN deployments as d \
            on tmp.deployment_id = d.deployment_id;\" | grep \'-\' | grep -v \'\\-\\-\'
        '''
        return self.execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)

    def fetch_all_active_instances(self):
        self.records = []
        results = self.instance_db.query_active_instances()
        for row in results:
            self.records.append(row)

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

            result = self.instance_db.check_instance_if_exists((instance_id, deployment_id, chute_id, host_ip, gpu_type))
            if len(result) == 0:
                self.instance_db.insert_instance((instance_id, deployment_id, chute_id, host_ip, gpu_type, created_at, gpu_count))

    def fetch_audit_latest_time(self):
        pod_name = self.chutes_audit_host['pod_name']
        today = datetime.date.today()
        command = f'sudo docker exec {pod_name} psql -U user chutes_audit -c \"SELECT max(started_at) FROM invocations;\" | grep {today}'
        (err, out) = self.execute_ssh_command(self.chutes_audit_host['host_ip'], self.chutes_audit_host['username'], command)
        if err != "":
            raise Exception(err)
        latest_time = out.strip()
        self.latest_time = latest_time

    def fetch_instance_compute(self, instance_id, latest_time, check_interval):
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
        (err, out) = self.execute_ssh_command(self.chutes_audit_host['host_ip'], self.chutes_audit_host['username'], command)
        if err != '':
            raise Exception(err)
        if out == '':
            return(0, 0, 0)
        else:
            return(out.split('|')[1].strip(), out.split('|')[3].strip(), out.split('|')[4].strip())

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

    def fetch_instance_deleted_at(self, deployment_id):
        pod_name = self.primary_host['pod_name']
        command = f'microk8s kubectl exec -n {pod_name} -- psql -U chutes chutes -c "select deployment_id, deleted_at from deployment_audit where deployment_id = \'{deployment_id}\';" | grep {deployment_id}'
        (err, out) = self.execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)
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
        (err, out) = self.execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)
        if err != '':
            raise Exception(err)
        if out == '':
            return False
        else:
            return True if len(out.strip()) > 0 else False

    def fetch_instance_chutes_compute_units(self, record):
        instance_id = record[0]
        if self.instances_chutes_compute_units.get(instance_id) is None:
            (invocation_count_1_hour, bounty_count_1_hour, compute_units_1_hour) = self.fetch_instance_compute(instance_id, self.latest_time, '1 hour')
            (invocation_count_1_day, bounty_count_1_day, compute_units_1_day) = self.fetch_instance_compute(instance_id, self.latest_time, '1 day')
            (invocation_count_7_days, bounty_count_7_days, compute_units_7_days) = self.fetch_instance_compute(instance_id, self.latest_time, '7 days')
            deleted_at = self.fetch_instance_deleted_at(record[1])
            mutex.acquire()
            try:
                self.instance_db.connect()
                print(f"Update {instance_id} deleted_at={deleted_at}")
                self.update_instance_deleted_at(deleted_at, instance_id)
                time.sleep(1)
            except Exception as e:
                print(e)
            finally:
                mutex.release()
                self.instance_db.close_connection()

            self.instances_chutes_compute_units[instance_id] = {
                "instance_id": instance_id,
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


    def fetch_instances_chutes_compute_units(self):
        self.instances_chutes_compute_units = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self.fetch_instance_chutes_compute_units, record): record for record in self.records}

            concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)


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
        display_instance_chutes(self.instances_chutes_compute_units, "Active Instances", "Active")


    def fetch_chutes(self):
        self.chutes = {}
        for instance, compute_units in self.instances_chutes_compute_units.items():
            if compute_units['chute_id'] not in self.chutes:
                self.chutes[compute_units['chute_id']] = [instance]
            else:
                self.chutes[compute_units['chute_id']].append(instance)


    def delete_deployment(self, deployment_id):
        pod_name = self.primary_host['pod_name']
        command = f' microk8s kubectl delete deployment chute-{deployment_id} -n chutes'
        print(command)
        return self.execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)


    def delete_inefficient_chutes(self, auto_delete: bool):
        print(self.delete_cfg)
        self.inefficient_chutes = {}
        for instance, compute_units in self.instances_chutes_compute_units.items():
            need_delete = False
            running_time = time.time() - time.mktime(time.strptime(compute_units['started_at'], "%Y-%m-%d %H:%M:%S.%f+00"))

            # compute_units_1_hour < self.delete_cfg["compute_units_for_hour"], if return True, need delete
            check_compute_units_1_hour = check_compute_units(running_time, self.delete_cfg["running_time_for_hour"], int(str(compute_units['compute_units_1_hour']).split('.')[0]), self.delete_cfg["compute_units_for_hour"])

            # compute_units_1_day < self.delete_cfg["compute_units_for_day"], if return True, need delete
            check_compute_units_1_day = check_compute_units(running_time, self.delete_cfg["running_time_for_day"], int(str(compute_units['compute_units_1_day']).split('.')[0]), self.delete_cfg["compute_units_for_day"])

            # compute_units_7_day < self.delete_cfg["compute_units_for_7day"], if return True, need delete
            check_compute_units_7_day = check_compute_units(running_time, self.delete_cfg["running_time_for_7day"], int(str(compute_units['compute_units_7_days']).split('.')[0]), self.delete_cfg["compute_units_for_7day"])

            # invocation_count_1_hour < self.delete_cfg["invocation_count_for_hour"], if return True, need delete
            check_invocation_count_1_hour = check_invocation_count(running_time, self.delete_cfg["running_time_for_hour"], int(compute_units['invocation_count_1_hour']), self.delete_cfg["invocation_count_for_hour"])

            # invocation_count_1_day < self.delete_cfg["invocation_count_for_day"], if return True, need delete
            check_invocation_count_1_day = check_invocation_count(running_time, self.delete_cfg["running_time_for_day"], int(compute_units['invocation_count_1_day']), self.delete_cfg["invocation_count_for_day"])

            # invocation_count_7_day < self.delete_cfg["invocation_count_for_hour"], if return True, need delete
            check_invocation_count_7_days = check_invocation_count(running_time, self.delete_cfg["running_time_for_7day"], int(compute_units['invocation_count_7_days']), self.delete_cfg["invocation_count_for_7day"])

            # chute_count > reserved_chute_count, if return True, need delete
            check_local_chute_count = check_chute_count(len(self.chutes[compute_units["chute_id"]]), self.delete_cfg["local_chute_reserved"])

            if (check_compute_units_1_hour or check_compute_units_1_day or check_compute_units_7_day \
                or check_invocation_count_1_hour or check_invocation_count_1_day or check_invocation_count_7_days) \
                and check_local_chute_count:
                need_delete = True

            if need_delete is False:
                continue
            if auto_delete:
                self.delete_deployment(compute_units["deployment_id"])
                self.chutes[compute_units["chute_id"]].remove(instance)
                print(f'Successfully deleted deployment: {compute_units["deployment_id"]}')
            else:
                self.inefficient_chutes[instance] = compute_units

        display_instance_chutes(self.inefficient_chutes, "Inefficient Chutes", "Chute ID")

def check_compute_units(running_time, least_running_time, compute_units, least_compute_units):
    is_running_time_valid = running_time >= least_running_time
    is_less_than_least_compute_units = compute_units < least_compute_units
    return is_running_time_valid and is_less_than_least_compute_units

def check_invocation_count(running_time, least_running_time, invocation_count, least_invocation_count):
    is_running_time_valid = running_time >= least_running_time
    is_less_than_least_invocation_count = invocation_count < least_invocation_count
    return is_running_time_valid and is_less_than_least_invocation_count

def check_chute_count(chute_count, chute_count_reserved):
    is_more_than_least_chute_reserved = chute_count > chute_count_reserved
    return is_more_than_least_chute_reserved


def display_instance_chutes(instances, title, sortby):
    t = PrettyTable(['Host IP', 'GPU Type', 'GPU Count', 'Instace ID', 'Chute ID', 'Deployment ID', 'Running Time', 'Active', 'Compute Units 1h', 'Compute Units 1d', 'Compute Units 7d', 'Invocation Count 1h', 'Invocation Count 1d', 'Invocation Count 7d', 'Bounty Count 7d'])
    t.title = title
    instances = instances
    sortby = sortby

    for instance, chutes in instances.items():
        t.add_row([
                chutes["host_ip"],
                chutes["model_short_ref"],
                chutes["gpu_count"],
                chutes["instance_id"][-12:],
                chutes["chute_id"][-12:],
                chutes["deployment_id"][-12:],
                str(datetime.timedelta(seconds = time.time() - time.mktime(time.strptime(chutes['started_at'], "%Y-%m-%d %H:%M:%S.%f+00")))).split('.')[0],
                False if chutes['deleted_at'] != 0 else True,
                str(chutes['compute_units_1_hour']).split('.')[0],
                str(chutes['compute_units_1_day']).split('.')[0],
                str(chutes['compute_units_7_days']).split('.')[0],
                chutes['invocation_count_1_hour'],
                chutes['invocation_count_1_day'],
                chutes['invocation_count_7_days'],
                chutes['bounty_count_7_days']
                ])
    print(t.get_string(sortby=sortby))


def get_cli_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="config which contains your miner hotkey, miner uid and machine info", required=True)
    parser.add_argument("-a", "--auto-delete", action="store_true", help="Delete deployment automatically")
    args = parser.parse_args()
    return args


def main():
    args = get_cli_args()
    auto_delete = args.auto_delete

    try:
        config = Config(args.config)
    except Exception as e:
        print(f'\33[31mFailed load config: {e}\33[0m')

    db_name = "chutes_deployments.db"
    instance_db = SQLiteInstance(db_name)
    instance_db.connect()
    instance_db.create_table()

    executor = Executor(config, instance_db)

    executor.insert_instances()
    executor.fetch_all_active_instances()
    instance_db.close_connection()

    executor.fetch_audit_latest_time()
    executor.fetch_instances_chutes_compute_units()

    executor.print_hosts_compute_units()
    executor.print_hosts_chutes_compute_units()

    executor.fetch_chutes()
    executor.delete_inefficient_chutes(auto_delete)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'\33[31mFailed run deployment: {e}\033[0m')
        traceback.print_exc()
