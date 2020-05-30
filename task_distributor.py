import json
import importlib
from collections import deque
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.db import models

" list of al the tasks need to limits "
TASK_NAMES = ["function1", "function2"]

" modules name as key and their functions as value "
MODULE_TASK_NAMES = {"module1": "function1", "module2": "function2"}

" Add the entry for the modules whose functions required resource_distributation "
MODULE_DISTRIBUTION = [
    {"module_name": "module1", "threashold_value": 3},
    {"module_name": "module2", "threshold_value": 2},
]

""" sender can be the model object which is storing the result of your task"""


@receiver(post_save, sender=None)
def result_collector(sender, instance, **kwargs):
    if instance.task_name in TASK_NAMES and (
        instance.status == "SUCCESS" or instance.status == "FAILURE"
    ):
        module_name = instance.task_name.split(".")[0]
        release(module_name)


def release(module_name):
    module_task = TaskDistribution.objects.get(module_name=module_name)
    running = deque(json.loads(module_task.running), maxlen=module_task.threshold)
    if len(running) is 0:
        return False
    module_task.semaphore = module_task.semaphore + 1
    print(
        f" Release -> Module Name : {module_name} SEMAPHORE : {module_task.semaphore}"
    )

    running.pop()

    if module_task.semaphore <= 0:
        suspend = deque(json.loads(module_task.suspend))
        params = suspend.pop()  # Add your own priority logic here
        running.append(params)
        module_task.suspend = json.dumps(list(suspend))

        _module, _function = MODULE_TASK_NAMES.get(module_name).split("-")
        module_import = importlib.import_module(_module)
        function = getattr(module_import, _function)
        function.delay(**params)

    module_task.running = json.dumps(list(running))
    module_task.save()
    return True


def acquire(module_name, **kwargs):
    module_task = TaskDistribution.objects.get(module_name=module_name)
    module_task.semaphore = module_task.semaphore - 1
    print(f"Module Name : {module_name} SEMAPHORE : {module_task.semaphore}")

    if module_task.semaphore < 0:
        suspend = deque(json.loads(module_task.suspend))
        suspend.append(kwargs)
        module_task.suspend = json.dumps(list(suspend))
        module_task.save()
        return False

    running = deque(json.loads(module_task.running), maxlen=module_task.threshold)
    running.append(kwargs)
    module_task.running = json.dumps(list(running))
    module_task.save()

    _module, _function = MODULE_TASK_NAMES.get(module_name).split("-")
    module_import = importlib.import_module(_module)
    function = getattr(module_import, _function)
    function.delay(**kwargs)

    return True


def create_module_task_distribution():
    """
    This functions creates TaskDistribution object for each module.
    Updates the threshold if object is created before
    :param : None
    :return: None
    """

    for module in MODULE_DISTRIBUTION:
        obj, created = TaskDistribution.objects.get_or_create(
            module_name=module.get("module_name"),
            defaults={"semaphore": module.get("threshold_value")},
        )
        if created:
            obj.running = "[]"
            obj.suspended = "[]"
            obj.threshold = module.get("threshold_value")
            obj.save()
        else:
            obj.threshold = module.get("threshold_value")
            obj.save()


""" The Model that stores the limit value of each module, and respective running, suspended processes value """


class TaskDistribution(models.Model):
    semaphore = models.IntegerField(default=1)
    running = models.TextField(default="[]", null=True)
    suspend = models.TextField(default="[]", null=True)
    module_name = models.TextField(null=False)


# This implementation is similar to BoundedSemaphores
if __name__ == "__main__":
    acquire("module1", **params)
