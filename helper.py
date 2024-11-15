import copy
import uuid
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from tqdm import tqdm


def get_id() -> int:
    return uuid.uuid4().int


def get_rollback_time_version(deltatable, version: int) -> int:
    tmp = copy.deepcopy(deltatable)
    start = datetime.now()
    tmp.rollback_version(version)
    end = datetime.now()
    return (end - start).total_seconds() * 1000


def get_rollback_time_transactions(deltatable, number: int) -> int:
    tmp = copy.deepcopy(deltatable)
    start = datetime.now()
    tmp.rollback_transactions(number)
    end = datetime.now()
    return (end - start).total_seconds() * 1000


def get_rollback_time_checkpoint(deltatable, number: int) -> int:
    tmp = copy.deepcopy(deltatable)
    start = datetime.now()
    tmp.rollback_checkpoint(number)
    end = datetime.now()
    return (end - start).total_seconds() * 1000


def insert_10_000_rows(deltatable):
    sizes = []
    for idx in tqdm(range(1_000)):
        deltatable.insert(
            {"name": "lucian", "role": "adc", "region": "runeterra", "species": "human"}
        )
        sizes.append(
            {
                "transactions": idx,
                "version mode": deltatable.get_size_of_versions(),
                "logs mode": deltatable.get_size_of_transactions(),
                "checkpoint mode": deltatable.get_size_of_checkpoints(),
            }
        )
    rollback_time = []
    for idx in tqdm(np.arange(start=1, stop=1_000, step=100)):
        rollback_time.append(
            {
                "transactions": idx,
                "version mode": get_rollback_time_version(deltatable, idx),
                "logs mode": get_rollback_time_transactions(deltatable, idx),
                "checkpoint mode": get_rollback_time_checkpoint(deltatable, idx),
            }
        )
    size_df = pd.DataFrame(sizes).set_index("transactions")
    rollback_time_df = pd.DataFrame(rollback_time).set_index("transactions")
    return size_df, rollback_time_df


def measure_rollback_time_and_size(deltatable):
    size_df, rollback_time_df = insert_10_000_rows(deltatable)
    fig, axes = plt.subplots(ncols=2, figsize=(15, 5))
    sns.lineplot(data=size_df, ax=axes[0]).set(title="Size", ylabel="bytes")
    sns.lineplot(data=rollback_time_df, ax=axes[1]).set(
        title="Rollback time", ylabel="ms"
    )
    fig.savefig("perf.png")
