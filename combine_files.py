import os

paths = []
for dirname, _, filenames in os.walk('data/archive'):
    for filename in filenames:
        paths.append(os.path.join(dirname, filename))

print(paths)