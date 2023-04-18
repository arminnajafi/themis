from dataclasses import dataclass


@dataclass
class DagConfig:
    scale_factor: int
    region: str
    stage: str
