import logging

CLUSTER_ID = 'cluster_id'
BENCHMARK_RUN_ID = 'benchmark_run_id'


def log_cluster_id(logger: logging.Logger, cluster_id: str) -> None:
    logger.info(f"{CLUSTER_ID} {cluster_id}")


def extract_cluster_id(log) -> str:
    assert CLUSTER_ID in log
    return log.split(CLUSTER_ID)[1].strip()


def log_benchmark_run_id(logger: logging.Logger, benchmark_run_id: str) -> None:
    logger.info(f"{BENCHMARK_RUN_ID} {benchmark_run_id}")


def extract_benchmark_run_id(log: str) -> str:
    assert BENCHMARK_RUN_ID in log
    return log.split(BENCHMARK_RUN_ID)[1].strip()
