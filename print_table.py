import time
import datetime
from prettytable import PrettyTable


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
                str(chutes['chutes_1_hour']).split('.')[0],
                str(chutes['chutes_1_day']).split('.')[0],
                str(chutes['chutes_7_days']).split('.')[0],
                chutes['invocation_count_1_hour'],
                chutes['invocation_count_1_day'],
                chutes['invocation_count_7_days'],
                chutes['bounty_count_7_days']
                ])
    print(t.get_string(sortby=sortby))
