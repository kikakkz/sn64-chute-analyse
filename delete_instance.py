import time

from get_args import get_cli_args
from get_args import Config
from remote_ssh import execute_ssh_command
from print_table import display_instance_chutes


class DeleteParam:
    def __init__(self, least_running_time_1_day, least_running_time_7_days, least_compute_units_1_day, least_compute_units_7_days, least_local_chute_count):
        self.least_running_time_1_day = least_running_time_1_day
        self.least_running_time_7_days= least_running_time_7_days
        self.least_compute_units_1_day = least_compute_units_1_day
        self.least_compute_units_7_days = least_compute_units_7_days
        self.least_local_chute_count = least_local_chute_count


def init_delete_config(config):
    least_running_time_1_day = config.get('least_running_time_1_day', '86400')
    least_running_time_7_days = config.get('least_running_time_7_days', '604800')
    least_compute_units_1_day = config.get('least_compute_units_1_day', '0')
    least_compute_units_7_days = config.get('least_compute_units_7_days', '0')
    least_local_chute_count = config.get('least_local_chute_count', '0')

    return DeleteParam(
        least_running_time_1_day,
        least_running_time_7_days,
        least_compute_units_1_day,
        least_compute_units_7_days,
        least_local_chute_count
        )


def fetch_chutes(instance_chutes):
    chutes = {}
    for instance, instance_info in instance_chutes.items():
        if instance_info['chute_id'] not in chutes:
            chutes[instance_info['chute_id']] = [instance]
        else:
            chutes[instance_info['chute_id']].append(instance)
    return chutes


def fetch_non_performance_chutes(delete_cfg, instance_chutes):
    chutes = fetch_chutes(instance_chutes)

    non_performance_instances = {}
    for instance, instance_info in instance_chutes.items():
        running_time = time.time() - time.mktime(time.strptime(instance_info['started_at'], "%Y-%m-%d %H:%M:%S.%f+00"))

        if check_chute_count_less_than_least(len(chutes[instance_info["chute_id"]]), delete_cfg.least_local_chute_count):
            continue

        chutes[instance_info["chute_id"]].remove(instance)

        if check_compute_units_not_performance(running_time = running_time, least_running_time = delete_cfg.least_running_time_1_day, compute_units = instance_info['compute_units_1_day'], least_compute_units = delete_cfg.least_compute_units_1_day):
            non_performance_instances[instance] = instance_info
    return non_performance_instances


def check_compute_units_not_performance(running_time, least_running_time, compute_units, least_compute_units):
    is_running_time_valid = running_time >= least_running_time
    is_less_than_least_compute_units = compute_units < least_compute_units
    return is_running_time_valid and is_less_than_least_compute_units


def check_chute_count_less_than_least(chute_count, least_local_chute_count):
    is_less_than_least = chute_count < least_local_chute_count
    return is_less_than_least


def delete_instance_from_k8s(non_performance_chutes, primary_host):
    pod_name = primary_host['pod_name']
    host_ip = primary_host['host_ip']

    for instance, instance_info in non_performance_chutes.items():
        deployment_id = instance_info['deployment_id']
        command = f' microk8s kubectl delete deployment chute-{deployment_id} -n chutes'
        return execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)


def print_non_performance_chutes(non_performance_chutes):
    title = "Non Performance Chutes"
    sortby = "Chute ID"
    display_instance_chutes(non_performance_chutes, title, sortby)


def execute_delete_instance(config, instance_chutes, primary_host):
    delete_cfg = init_delete_config(config)
    
    non_performance_chutes = fetch_non_performance_chutes(delete_cfg, instance_chutes)

    print_non_performance_chutes(non_performance_chutes)
    delete_instance_from_k8s(non_performance_chutes, primary_host)


if __name__ == '__main__':
    args = get_cli_args()
    try:
        config = Config(args.config)
    except Exception as e:
        print(f'\33[31mFailed load config: {e}\33[0m')

    delete_config = config.fetch_delete_cfg()
    primary_host = config.primary_host()


    instance_chutes = {
        '4c4539fa-3eb0-42d1-935b-9a85d14c766a': {
            'instance_id': '4c4539fa-3eb0-42d1-935b-9a85d14c766a', 
            'compute_units_1_hour': 0, 
            'compute_units_1_day': 0, 
            'compute_units_7_days': 0, 
            'bounty_count_1_hour': 0, 
            'bounty_count_1_day': 0, 
            'bounty_count_7_days': 0, 
            'invocation_count_1_hour': 0, 
            'invocation_count_1_day': 0, 
            'invocation_count_7_days': 0, 
            'deployment_id': 'bfaf6262-7faf-48a4-a441-e18dfed152df', 
            'chute_id': 'c18e50b7-a1d0-5b77-8d78-ea27d9746317', 
            'host_ip': '51.20.78.94', 
            'model_short_ref': 'l4', 
            'started_at': '2025-03-10 09:32:21.522186+00', 
            'gpu_count': 1, 
            'deleted_at': 0
            }
    }
    # run test
    # execute_delete_instance(delete_config, instance_chutes, primary_host)
