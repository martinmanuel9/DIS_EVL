import yaml

with open('environment.yml') as f:
    env = yaml.safe_load(f)

with open('requirements.txt', 'w') as f:
    if 'dependencies' in env:
        for dep in env['dependencies']:
            if isinstance(dep, str):
                # Skip conda packages
                continue
            elif isinstance(dep, dict) and 'pip' in dep:
                for pip_pkg in dep['pip']:
                    f.write(f"{pip_pkg}\n")
