import os
from datetime import datetime
import scipy.stats as st
from tqdm import tqdm
import json

DISTRIBS = {
    "arrival_times": {
        "accel": "beta",
        "bigmem": "f",
        "learning": "exponweib",
        "longjobs": "norm"
    },
    "elapsed_times": {
        "accel": "chi2",
        "bigmem": "f",
        "learning": "weibull_min",
        "longjobs": "weibull_min"
    },
    "limit_times": {
        "accel": "norm",
        "bigmem": "f",
        "learning": "dweibull",
        "longjobs": "dweibull"
    }
}


def find_parameters(distribution, data):
    dist = getattr(st, distribution)
    params = dist.fit(data)
    return params


result = {

}

filepaths = []

for root, dirs, files in os.walk("csv"):
    for name in files:
        filepaths.append(os.path.join(root, name))

for filepath in tqdm(filepaths, desc="Loading..."):
    
    submit_times = {}

    elapsed_times = {}

    cpu_counts = {}

    memory_usages = {}

    limit_times = {}

    for k in DISTRIBS['arrival_times'].keys():
        submit_times[k] = []
        elapsed_times[k] = []
        cpu_counts[k] = []
        memory_usages[k] = []
        limit_times[k] = []

    with open(filepath, "r") as f:
        for i, line in enumerate(f):
            if i != 0:
                parts = line.split(",")
                for k in DISTRIBS['arrival_times'].keys():
                    if k in parts[-2]:
                        submit_times[k].append(datetime.strptime(
                            parts[9], '%Y/%m/%d %H:%M:%S').timestamp())

                        elapsed_txt = parts[12]
                        elapsed_parts = list(map(int, elapsed_txt.split(":")))
                        elapsed_time = elapsed_parts[0] * 3600 + \
                            elapsed_parts[1] * 60 + elapsed_parts[2]
                        elapsed_times[k].append(elapsed_time)

                        cpu_counts[k].append(parts[8])
                        memory_usages[k].append(parts[7])
                        
                        limit_txt = parts[-5]
                        limit_parts = list(map(int, limit_txt.split(":")))
                        limit_time = limit_parts[0] * 3600 + \
                            limit_parts[1] * 60 + limit_parts[2]
                        limit_times[k].append(limit_time)



    cpu_distributions = {}

    for k,v in cpu_counts.items():
        cpu_distributions[k] = {}
        unique = set(v)
        for elem in unique:
            cpu_distributions[k][elem] = 0

        for internal in v:
            cpu_distributions[k][internal] += 1

        for elem in unique:
            cpu_distributions[k][elem] /= len(v)

    memory_distributions = {}
    for k,v in memory_usages.items():
        memory_distributions[k] = {}
        unique = set(v)
        for elem in unique:
            memory_distributions[k][elem] = 0

        for internal in v:
            memory_distributions[k][internal] += 1

        for elem in unique:
            memory_distributions[k][elem] /= len(v)
    

    for i, v in submit_times.items():
        v.sort()

    inter_submits = {}
    for k in DISTRIBS['arrival_times'].keys():
        inter_submits[k] = [submit_times[k][i+1] - submit_times[k][i] for
                            i in range(len(submit_times[k])-1)]
    
    name = filepath.split('/')[-1]

    result[name] = {}
    result[name]['arrival_times'] = {}
    result[name]['elapsed_times'] = {}
    result[name]['limit_times'] = {}
    result[name]['cpu_distributions'] = cpu_distributions
    result[name]['memory_distributions'] = memory_distributions
    for partition in DISTRIBS['arrival_times'].keys():
        result[name]['limit_times'][partition] = {}
        result[name]['limit_times'][partition]['distribution'] = DISTRIBS["limit_times"][partition]
        result[name]['limit_times'][partition]['parameters'] = find_parameters(
            DISTRIBS["limit_times"][partition], limit_times[partition])
        
        result[name]['arrival_times'][partition] = {}
        result[name]['arrival_times'][partition]['distribution'] = DISTRIBS["arrival_times"][partition]
        result[name]['arrival_times'][partition]['parameters'] = find_parameters(
            DISTRIBS["arrival_times"][partition], inter_submits[partition])
        result[name]['elapsed_times'][partition] = {}
        result[name]['elapsed_times'][partition]['distribution'] = DISTRIBS["elapsed_times"][partition]
        result[name]['elapsed_times'][partition]['parameters'] = find_parameters(
            DISTRIBS["elapsed_times"][partition], elapsed_times[partition])
    

with open("distributions.json", 'w') as f:
    json.dump(result, f)

