from tqdm import tqdm
import re
import os

regex_str = r'(((\d+)(-))?(\d+):(\d+)(:\d+)?(\.\d+)?:?\+?) +(((\d+)(-))?(\d+):(\d+)(:\d+)?(\.\d+)?:?\+?) +(\d+) +( +Unknown|(\d+-\d+-\d+)T\d+:\d+:\d+) +( +Unknown|(\d+-\d+-\d+)T\d+:\d+:\d+) +( +Unknown|(\d+-\d+-\d+)T\d+:\d+:\d+) +(\d+)? +(\d+(.\d+)?[a-zA-Z]+) +(\d+) +( +Unknown|(\d+-\d+-\d+)T\d+:\d+:\d+) +(((\d+)(-))?(\d+):(\d+)(:\d+)?(\.\d+)?:?\+?)? (\d+([^ ]+)) +(((\d+)(-))?(\d+):(\d+)(:\d+)?(\.\d+)?:?\+?) +([a-z]+(-\d+)?)? +([A-Z]+(\+)?)'

regex_data = re.compile(regex_str)

date_regex = re.compile(r'( +(Unknown)|(\d+-\d+-\d+)T(\d+:\d+:\d+))')

memory_regex = re.compile(r'((\d+(.\d+)?)([a-zA-Z]+))')

time_regex = re.compile(r'(((\d+)-)?((\d+):)?(\d+):(\d+)(\.(\d+))?)')


def parse_entry(line):
    entry = []
    line = line.strip()
    try:
        groups = regex_data.match(line).groups()
    except:
        print("WARNING!!! ")
        return None

    entry = [groups[i]
        for i in [0, 8, 16, 17, 19, 21, 23, 24, 26, 27, 29, 37, 39, 47, 49]]
    return entry


def write_entry(entry, descriptors):
    '''if entry[-1] != "COMPLETED":
                continue'''
    if "." in entry[11] or "_" in entry[11]:
        return
    if entry[12] == "00:00:00":
        return
    index = int(entry[-6].split("T")[0].split("-")[1])-1
    fout = descriptors[index]
    for index, value in enumerate(entry):
        if value == None:
            value = ""
        matchdate = date_regex.match(value) if index in [3, 4, 5, 9] else False
        if matchdate:
            groups = matchdate.groups()
            if (not groups[1]):
                result_date = groups[2].replace(
                    "-", "/") + " " + groups[3]
                fout.write(result_date)
            else:
                fout.write("")
        else:
            matchmem = memory_regex.match(
                value) if index in [7] else False
            if matchmem:
                groups = matchmem.groups()
                numcpus = int(entry[2])
                memory_units = groups[3]
                memnum = float(groups[1])
                if memory_units[1] == 'G':
                    memnum *= 1024
                if memory_units[1] == 'c':
                    memnum /= numcpus
                fout.write(str(memnum))
            else:
                matchtime = time_regex.match(value) if index in [0, 1, 10, 12] else False
                if matchtime:
                    groups = matchtime.groups()
                    days, hours, minutes, seconds, milis = (0, 0, 0, 0,0)
                    if groups[2]:
                        days = int(groups[2])
                    if groups[4]:
                        hours = int(groups[4])
                    if groups[5]:
                        minutes = int(groups[5])
                    if groups[6]:
                        seconds = int(groups[6])
                    if groups[7]:
                        # milis = int(groups[7])
                        pass
                    hoursstr = str(days * 24 + hours).zfill(2)
                    minsstr = str(minutes).zfill(2)
                    secstr = str(seconds).zfill(2)
                    finaltime = hoursstr + ":" + minsstr + ":" + secstr
                    fout.write(finaltime)
                else:
                    fout.write(value)
        if index != len(entry) - 1:
            fout.write(",")
    fout.write("\n")


def reformat(filepath_in, descriptors):
    line_id = 0
    data = []
    with open(filepath_in, "r") as fin:
        for line in tqdm(fin, desc="Reading from {}...".format(filepath_in)):
            line_id += 1
            if line_id <= 2:
                continue
            entry = parse_entry(line)
            if entry != None:
                write_entry(entry, descriptors)


    # CPUTIME = 0
    # Totalcpu = 8
    # AllocCPUS = 16
    # Start = 17
    # End = 19
    # Eligible = 21
    # Priority = 23
    # Reqmem = 24
    # cpureq = 25
    # submit = 26
    # job_id = 36
    # elapsed = 41
    # partition = 49
    # status = 51

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov",
    "Dec"]

descriptors = []
for month in MONTHS:
    descriptors.append(open("csv/"+month, "w"))
    descriptors[-1].write("\n")

for month in MONTHS:
    reformat("data/"+month, descriptors)

for desc in descriptors:
    desc.close()