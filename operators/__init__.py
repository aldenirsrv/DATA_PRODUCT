import pkgutil, importlib

TASK_FUNCTIONS = {}

package = __name__
for _, module_name, _ in pkgutil.iter_modules(__path__):
    module = importlib.import_module(f"{package}.{module_name}")
    for attr in dir(module):
        if attr.endswith("_TASKS"):
            TASK_FUNCTIONS.update(getattr(module, attr))

print(f"✅ Auto-registered {len(TASK_FUNCTIONS)} task functions from {package}.")