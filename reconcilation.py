import time

from func_timeout import func_timeout, FunctionTimedOut
from config import Config
from remote_ssh import execute_ssh_command
from print_table import display_instance_metrics


class Config:
    def __init__(self, config):
        self.least_running_time_1_day = config.get('least_running_time_1_day', '86400')
        self.least_running_time_7_days= config.get('least_running_time_7_days', '604800')
        self.least_compute_units_1_day = config.get('least_compute_units_1_day', '0')
        self.least_compute_units_7_days = config.get('least_compute_units_7_days', '0')
        self.least_invocation_count_1_day = config.get('least_invocation_count_1_day', 0)
        self.least_invocation_count_7_days = config.get('least_invocation_count_7_days', 0)
        self.least_local_chute_count = config.get('least_local_chute_count', '0')


class Reconcilation:
    def __init__(self, config, instances_metrics, primary_host, auto_delete):
        self.instances_metrics = instances_metrics
        self.primary_host = primary_host
        self.config = Config(config)
        self.auto_delete = auto_delete


    def fetch_chutes(self):
        self.chutes = {}
        for instance, instance_info in self.instances_metrics.items():
            if instance_info['chute_id'] not in self.chutes:
                self.chutes[instance_info['chute_id']] = [instance]
            else:
                self.chutes[instance_info['chute_id']].append(instance)


    def fetch_low_performance_instances(self):
        self.fetch_chutes()

        self.low_performance_instances = {}
        for instance, instance_info in self.instances_metrics.items():
            running_time = time.time() - time.mktime(time.strptime(instance_info['started_at'], "%Y-%m-%d %H:%M:%S.%f+00"))

            if self.check_least_chute_count(len(self.chutes[instance_info["chute_id"]]), self.config.least_local_chute_count):
                continue


            if self.check_low_compute_units(running_time = running_time, least_running_time = self.config.least_running_time_1_day, \
                    compute_units = instance_info['1_day']['compute_units'], least_compute_units = self.config.least_compute_units_1_day):
                self.low_performance_instances[instance] = instance_info
                self.chutes[instance_info["chute_id"]].remove(instance)
                continue


            if self.check_low_invocation_count(running_time = running_time, least_running_time = self.config.least_running_time_1_day, \
                    invocation_count = instance_info['1_day']['invocation_count'], least_invocation_count = self.config.least_invocation_count_1_day):
                self.low_performance_instances[instance] = instance_info
                self.chutes[instance_info["chute_id"]].remove(instance)
                continue


    def check_low_compute_units(self, running_time, least_running_time, compute_units, least_compute_units):
        is_running_time_valid = running_time >= least_running_time
        is_less_than_least_compute_units = int(str(compute_units).split('.')[0]) < int(least_compute_units)
        return is_running_time_valid and is_less_than_least_compute_units
 

    def check_low_invocation_count(self, running_time, least_running_time, invocation_count, least_invocation_count):
        is_running_time_valid = running_time >= least_running_time
        is_less_than_least_invocation_count = int(str(invocation_count).split('.')[0]) < int(least_invocation_count)
        return is_running_time_valid and is_less_than_least_invocation_count


    def check_least_chute_count(self, chute_count, least_local_chute_count):
        is_less_than_least = chute_count < least_local_chute_count
        return is_less_than_least


    def delete_low_performance_from_k8s(self, selected_instances):
        pod_name = self.primary_host['pod_name']
        host_ip = self.primary_host['host_ip']

        for instance, instance_info in selected_instances.items():
            deployment_id = instance_info['deployment_id']
            command = f' microk8s kubectl delete deployment chute-{deployment_id} -n chutes'
            return execute_ssh_command(self.primary_host['host_ip'], self.primary_host['username'], command)


    def prompt_user_input(self):
        selected_instances = {}

        try:
            user_input = func_timeout(300, lambda: input("Enter the instances to remove (separated by spaces or commas): "))
        except:
            user_input = ""

        for instance in user_input.split(' '):
            selected_instance = next((k for k in self.low_performance_instances if str(instance) in k), None)
            if len(selected_instance) > 0:
                selected_instances[selected_instance] = self.low_performance_instances[selected_instance]

        return selected_instances


    def do(self):
        self.fetch_low_performance_instances()

        display_instance_metrics(self.low_performance_instances, "Low Performance Chutes", "Chute ID")

        if self.auto_delete:
            selected_instances = self.low_performance_instances
        else:
            selected_instances = self.prompt_user_input()
            display_instance_metrics(selected_instances, "Selected low performance instances for Deletion", "Chute ID")

            try:
                func_timeout(10, lambda: input("Preess Enter to confirm deletion..."))
            except:
                user_input = None

        if len(selected_instances) == 0:
            return

        self.delete_low_performance_from_k8s(selected_instances)


if __name__ == '__main__':
    args = get_cli_args()
    try:
        config = Config(args.config)
    except Exception as e:
        print(f'\33[31mFailed load config: {e}\33[0m')

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
    # deletion = Reconcilation(config.reconcilation(), instance_chutes, config.primary_host(), config.auto_delete())
    # reconcilation.do()
