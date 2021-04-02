from datetime import datetime
import matplotlib.pyplot as plt
import scipy.stats as st


import numpy
import statistics

dates = []

with open("csv/Jan", "r") as f:
    for i,line in enumerate(f):
        if i != 0:
            parts = line.split(",")
            if "accel" in parts[-2]:
                dates.append(datetime.strptime(parts[9], '%Y/%m/%d %H:%M:%S').timestamp())

dates.sort()

inter_arrival = [dates[i+1] - dates[i] for i in range(len(dates)-1)]
print(inter_arrival)
#plt.hist(inter_arrival)
#plt.show()

#inter_arrival = list(filter(lambda x: x > 4, inter_arrival))

#print(inter_arrival)

inter_arrival_delayed = [inter_arrival[i+1] for i in range(len(inter_arrival)-1)]

simulated = numpy.round(st.exponweib.rvs(1.3, 0.5, loc=-7, scale=1839,size=len(inter_arrival)))
simulated_delated = [simulated[i+1] for i in range(len(simulated) -1)]


def get_best_distribution(data):
    dist_names = ["alpha", "beta", "chi", "chi2", "dgamma", "dweibull", "erlang", "expon", "exponnorm", "exponweib", "exponpow", "f", "pareto", "powernorm", "norm", "t", "trapezoid", "triang", "truncexpon", "truncnorm", "weibull_min", "weibull_max"]
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

print(get_best_distribution(inter_arrival))


'''plt.hist(simulated,alpha=0.5)
plt.hist(inter_arrival,alpha=0.5)

plt.show()'''

'''plt.scatter(inter_arrival[:-1], inter_arrival_delayed, alpha=0.1)
plt.scatter(simulated[:-1], simulated_delated,alpha=0.05)
#print(simulated)

plt.show()'''