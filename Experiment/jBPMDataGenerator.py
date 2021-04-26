from jBPM_REST import createProcessInstance, retrieveActiveTasks, startActiveTask, completeActiveTask, claimActiveTask
import json
from time import sleep
import random


if __name__ == "__main__":
    create_instance_payload = {
        "employee": "wbadmin",
        "reason": "test"
    }

    number_of_instances = 1466
    success_count = 0
    for x in range(number_of_instances):
        #print("Processing instance #" + str(x))
        print(".", end="", flush=True)
        try:
            process_instance_id = createProcessInstance(json.dumps(create_instance_payload))
            #print(process_instance_id)

            while True:
                task_list = retrieveActiveTasks(process_instance_id)
                sleep(0.05)
                if task_list is None:
                    break
                random.shuffle(task_list)
                for task in task_list:
                    #print('task', task)
                    task_id = task['task-id']
                    if len(str(task_id)) == 0:
                        continue  # just in case~
                    task_status = task['task-status']
                    if task_status == "Reserved":
                        #print("status is Reserved, perform start now!")
                        startActiveTask(task_id)
                    elif task_status == "InProgress":
                        #print("status is InProgress, perform complete now!")
                        submit_payload = {"performance": 7}
                        completeActiveTask(task_id, json.dumps(submit_payload))
                    elif task_status == "Ready":
                        #print("status is Ready, perform claim now!")
                        claimActiveTask(task_id)
        except Exception as ex:
            process_instance_id = str(process_instance_id) if not None else ""
            print("Instance generation failed: " + str(x) + "process_instance_id: " + process_instance_id)
            continue
        success_count += 1

    print("\nGenerated " + str(success_count) + " instances within jBPM. Good Job!")
