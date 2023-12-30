import os
from subprocess import run

# List of required modules
modules = [
    'selenium',
    'numpy',
    'pandas',
    'xlwings',
    'openpyxl',
    'keyboard',
    'tqdm',
    'pyodbc',
    'geckodriver_autoinstaller',
    'automation',
    'tensorflow',
]

# Install the required modules
for module in modules:
    run(['pip', 'install', module])

# Generate requirements.txt using pip freeze
run(['pip', 'freeze', '>', 'requirements.txt'], shell=True)
