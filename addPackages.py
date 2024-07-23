import subprocess

with open('requirements.txt') as f:
    for line in f:
        package = line.strip()
        if package:
            subprocess.run(['poetry', 'add', package])
