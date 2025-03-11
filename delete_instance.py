import time

from get_args import get_cli_args
from get_args import Config
from remote_ssh import execute_ssh_command
from print_table import display_instance_chutes


class DeleteParam:
    def __init__(self, config): 
        self.least_running_time_1_day = config.get('least_running_time_1_day', '86400')
        self.least_running_time_7_days= config.get('least_running_time_7_days', '604800')
        self.least_compute_units_1_day = config.get('least_compute_units_1_day', '0')
        self.least_compute_units_7_days = config.get('least_compute_units_7_days', '0')
        self.least_local_chute_count = config.get('least_local_chute_count', '0')


class Deletion:
    def __init__(self, config, instances, primary_host):
        self.config = config
        self.instances = instances
        self.primary_host = primary_host


    def fetch_chutes(self):
        self.chutes = {}
        for instance, instance_info in self.instances.items():
            if instance_info['chute_id'] not in self.chutes:
                self.chutes[instance_info['chute_id']] = [instance]
            else:
                self.chutes[instance_info['chute_id']].append(instance)


    def fetch_non_performance_chutes(self):
        self.fetch_chutes()

        self.non_performance_instances = {}
        for instance, instance_info in self.instances.items():
            running_time = time.time() - time.mktime(time.strptime(instance_info['started_at'], "%Y-%m-%d %H:%M:%S.%f+00"))

            if self.check_chute_count_less_than_least(len(self.chutes[instance_info["chute_id"]]), self.delete_cfg.least_local_chute_count):
                continue

            self.chutes[instance_info["chute_id"]].remove(instance)

            if self.check_compute_units_not_performance(running_time = running_time, least_running_time = self.delete_cfg.least_running_time_1_day, compute_units = instance_info['compute_units_1_day'], least_compute_units = self.delete_cfg.least_compute_units_1_day):
                self.non_performance_instances[instance] = instance_info


    def check_compute_units_not_performance(self, running_time, least_running_time, compute_units, least_compute_units):
        is_running_time_valid = running_time >= least_running_time
        is_less_than_least_compute_units = compute_units < least_compute_units
        return is_running_time_valid and is_less_than_least_compute_units
 

    def check_chute_count_less_than_least(self, chute_count, least_local_chute_count):
        is_less_than_least = chute_count < least_local_chute_count
        return is_less_than_least


    def delete_instance_from_k8s(self):
        pod_name = self.primary_host['pod_name']
        host_ip = self.primary_host['host_ip']

        for instance, instance_info in self.non_performance_instances.items():
            deployment_id = instance_info['deployment_id']
            command = f' microk8s kubectl delete deployment chute-{deployment_id} -n chutes'
            return execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)


    def print_non_performance_chutes(self):
        title = "Non Performance Chutes"
        sortby = "Chute ID"
        display_instance_chutes(self.non_performance_instances, title, sortby)


    def execute_delete_instance(self):
        self.delete_cfg = DeleteParam(self.config)
        self.fetch_non_performance_chutes()

        self.print_non_performance_chutes()
        self.delete_instance_from_k8s()


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
    # deletion = Deletion(delete_config, instance_chutes, primary_host)
    # deletion.execute_delete_instance()
