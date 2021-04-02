import simpy
import json
import scipy.stats as st
import random
import matplotlib.pyplot as plt

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov",
    "Dec"]

with open("distributions.json", 'r') as file:
    DISTRIBUTION_DATA = json.load(file) 

PARTITIONS = ['accel', 'bigmem', 'learning', 'longjobs']

def time_to_month(time):
    return MONTHS[int(time / (3600 * 24 * 30))]

def simulate_time(kind, month, partition):
    partition_data = DISTRIBUTION_DATA[month][kind][partition]
    dist = getattr(st, partition_data["distribution"])
    params = partition_data["parameters"]
    val = dist.rvs(*params)
    return val if val > 0 else 0

def simulate_from_empirical(kind, month, partition):
    partition_data = DISTRIBUTION_DATA[month][kind][partition]
    uniform = random.random()
    accum = 0
    for value, probability in partition_data.items():
        accum += probability
        if uniform < accum:
            return float(value)

CPU_USAGES = []
QUEUE_SIZES = []
RAM_USAGES = []
DURATIONS = []
QUEUE_SIZE = 0

def job(env,partition,cpu_cores,system_mem):
    global QUEUE_SIZE
    month = time_to_month(env.now)
    job_duration = simulate_time("elapsed_times",month,
        partition)
    limit_time = simulate_time("limit_times",month, partition)
    job_duration = job_duration if job_duration < limit_time else limit_time
    DURATIONS.append(job_duration)
    req_mem = simulate_from_empirical("memory_distributions",month,partition)
    req_cores = simulate_from_empirical("cpu_distributions", month,partition)
    allocated_resoures = False
    QUEUE_SIZE += 1
    while not allocated_resoures:
        yield cpu_cores.get(req_cores)
        if system_mem.level >= req_mem:
            yield system_mem.get(req_mem)
            allocated_resoures = True
        else:
            yield cpu_cores.put(req_cores)
    QUEUE_SIZE -= 1

    '''print("Job started in {} at {}, requiring {} cores and {} ram".format(
        partition,
        env.now,
        req_cores,
        req_mem
    ))'''
    yield env.timeout(job_duration)
    yield system_mem.put(req_mem)
    yield cpu_cores.put(req_cores)
    '''print("Job finished in {} at {}, requiring {} cores and {} ram".format(
        partition,
        env.now,
        req_cores,
        req_mem
    ))'''

COUNT = 0
def job_generator(env,partition,cpu_cores,system_mem):
    global COUNT
    while True:
        inter_arrival = simulate_time("arrival_times", 
            time_to_month(env.now), partition)
        yield env.timeout(inter_arrival)
        COUNT += 1
        env.process(job(env, partition,cpu_cores,system_mem))


def logger(env):
    count = 0
    while True:
        count += 1
        yield env.timeout(3600 * 24)
        CPU_USAGES.append(cpu_cores.level)
        RAM_USAGES.append(system_mem.level)
        QUEUE_SIZES.append(QUEUE_SIZE)
        print(count)



env = simpy.Environment()
cpu_cores = simpy.Container(env, 464, 464)
system_mem = simpy.Container(env, 1.6 * 1024 * 1024, 1.6 * 1024 * 1024)
for partition in PARTITIONS:
    env.process(job_generator(env, partition,cpu_cores,system_mem))
env.process(logger(env))


env.run(until=3600*24*30*12)

fig, (ax1, ax2, ax3) = plt.subplots(1,3)

print(COUNT)
ax1.set_title("Available CPU cores")
ax1.set_xlabel("Days")
ax1.plot(CPU_USAGES)
ax2.plot(RAM_USAGES)
ax2.set_title("Available RAM (Mb)")
ax2.set_xlabel("Days")
ax3.plot(QUEUE_SIZES)
ax3.set_title("Queue size (Jobs)")
ax3.set_xlabel("Days")
plt.show()
