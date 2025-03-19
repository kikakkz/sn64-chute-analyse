import time
import datetime
from prettytable import PrettyTable


def display_instance_metrics(instances, title, sortby):
    t = PrettyTable(['Host IP', 'GPU Type', 'GPU Count', 'Model Name', 'Instace ID', 'Chute ID', 'Deployment ID', 'Running Time', 'Active', 'CU 1h', 'CU 1d', 'CU 7d', 'IC 1h', 'IC 1d', 'IC 7d', 'Bounty 7d'])
    t.title = title
    instances = instances
    sortby = sortby

    for instance, metrics in instances.items():
        t.add_row([
                metrics["host_ip"],
                metrics["gpu_type"],
                metrics["gpu_count"],
                metrics["model_name"],
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


def display_rate_limit_chutes_metrics(chutes, title, sortby):
    t = PrettyTable(['Chute ID', 'day 1 Rate Limit', 'day 1 Total Invocation', 'day 2 Rate Limit', 'day 2 Total Invocation', 'day 3 Rate Limit', 'day 3 Total Invocation', 'Model Name'])
    t.title = title
    chutes = chutes
    sortby = sortby

    for chute, metrics in chutes.items():
        chute_data = [chute]
        t.add_row([
            chute,
            metrics['day_1']['rate_limit'],
            metrics['day_1']['total_invocation'],
            metrics['day_2']['rate_limit'],
            metrics['day_2']['total_invocation'],
            metrics['day_3']['rate_limit'],
            metrics['day_3']['total_invocation'],
            metrics['model_name']
            ])
    print(t.get_string(sortby=sortby))
