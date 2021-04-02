from datetime import datetime
import matplotlib.pyplot as plt
import scipy.stats as st


import numpy
import statistics

times = []

with open("csv/Sep", "r") as f:
    for i,line in enumerate(f):
        if i != 0:
            parts = line.split(",")
            if "bigmem" in parts[-2]:
                elapsed_txt = parts[-5] #-5 #-3
                elapsed_parts = list(map(int,elapsed_txt.split(":")))
                elapsed_time = elapsed_parts[0] * 3600 + elapsed_parts[1] * 60 + elapsed_parts[2]
                times.append(elapsed_time)

plt.hist(times)
plt.show()

def get_best_distribution(data):
    dist_names = ["alpha", "beta", "chi", "chi2", "dgamma", "dweibull", "erlang", "expon", "exponnorm", "exponweib", "exponpow", "f", "pareto", "powernorm", "norm", "t", "trapezoid", "triang", "truncexpon", "truncnorm", "weibull_min", "weibull_max", "uniform"]
    dist_results = []
    params = {}
    for dist_name in dist_names:
        dist = getattr(st, dist_name)
        param = dist.fit(data)

        params[dist_name] = param
        # Applying the Kolmogorov-Smirnov test
        D, p = st.kstest(data, dist_name, args=param)
        print("p value for "+dist_name+" = "+str(p))
        dist_results.append((dist_name, p))

    # select the best fitted distribution
    best_dist, best_p = (max(dist_results, key=lambda item: item[1]))
    # store the name of the best fit and its p value

    print("Best fitting distribution: "+str(best_dist))
    print("Best p value: "+ str(best_p))
    print("Parameters for the best fit: "+ str(params[best_dist]))

    return best_dist, best_p, params[best_dist]

print(get_best_distribution(times))

#print(times)

'''plt.hist(simulated,alpha=0.5)
plt.hist(inter_arrival,alpha=0.5)

plt.show()'''

'''plt.scatter(inter_arrival[:-1], inter_arrival_delayed, alpha=0.1)
plt.scatter(simulated[:-1], simulated_delated,alpha=0.05)
#print(simulated)

plt.show()'''