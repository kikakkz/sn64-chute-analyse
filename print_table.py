import time
import datetime
from prettytable import PrettyTable


def display_instance_metrics(instances, title, sortby):
    t = PrettyTable(['Host IP', 'GPU Type', 'GPU Count', 'Instace ID', 'Chute ID', 'Deployment ID', 'Running Time', 'Active', 'Compute Units 1h', 'Compute Units 1d', 'Compute Units 7d', 'Invocation Count 1h', 'Invocation Count 1d', 'Invocation Count 7d', 'Bounty Count 7d'])
    t.title = title
    instances = instances
    sortby = sortby

    for instance, metrics in instances.items():
        t.add_row([
                metrics["host_ip"],
                metrics["gpu_type"],
                metrics["gpu_count"],
                metrics["instance_id"][-12:],
                metrics["chute_id"][-12:],
                metrics["deployment_id"][-12:],
                str(datetime.timedelta(seconds = time.time() - time.mktime(time.strptime(metrics['started_at'], "%Y-%m-%d %H:%M:%S.%f+00")))).split('.')[0],
                False if metrics['deleted_at'] != 0 else True,
                str(metrics['1_hour']['compute_units']).split('.')[0],
                str(metrics['1_day']['compute_units']).split('.')[0],
                str(metrics['7_days']['compute_units']).split('.')[0],
                metrics['1_hour']['invocation_count'],
                metrics['1_day']['invocation_count'],
                metrics['7_days']['invocation_count'],
                metrics['7_days']['bounty']
                ])
    print(t.get_string(sortby=sortby))
