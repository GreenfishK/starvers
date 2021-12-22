import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from datetime import timezone
import socket

hostname = socket.gethostname()


def plot_mat_lookup_queries(timestamp: datetime, policy: str, triple_pattern: str):
    """
    Version materialisation queries with simple lookups for IC, CB and TB approaches.
    :param triple_pattern: s, p, o, sp, po, so, spo
    :param policy: ic, tb or cb
    :param timestamp:
    :return:
    """
    # TODO: make a list of policies as argument and pass tb and tb_star. For each policy a curve should be plotted for the respective set of queries.

    ts_formatted = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
    output_dir = "~/.BEAR/output/time/bearb-hour/{hostname}-{timestamp}".format(hostname=hostname,
                                                                                timestamp=ts_formatted)
    filename = "time-{policy}-mat-lookup_queries_{triple_pattern}.csv".format(policy=policy,
                                                                              triple_pattern=triple_pattern)
    df = pd.read_csv("{dir}/{fn}".format(dir=output_dir, fn=filename), delimiter=" ", skiprows=1,
                     names=['ver', 'min', 'mean', 'max', 'stddev', 'cnt_queries', 'total_time_in_ms'])

    ax = plt.gca()
    df.plot(kind="line", x="ver", y="total_time_in_ms", ax=ax)
    ax.set_xlabel("Version")
    ax.set_ylabel("total query time (in ms)")

    plt.show()

# simple lookups, 49 queries
plot_mat_lookup_queries(datetime(2021, 12, 8, 22, 11, 19), "tb", "p")

# other triple patterns, 13 queries
plot_mat_lookup_queries(datetime(2021, 12, 8, 22, 11, 19), "tb", "po")
