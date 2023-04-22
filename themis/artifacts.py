import click
import os
import logging

logger = logging.getLogger(__name__)

ARTIFACTS = {
    "spark-iceberg-tpcds-datagen": {
        "type": "gradle",
        "command": "./gradlew :themis-spark-iceberg:shadowJar",
        "artifact": "spark-iceberg/build/libs/themis-spark-iceberg-*-all.jar"
    }
}

THEMIS_ARTIFACTS_PATH = "themis/resources/artifacts"

@click.group()
def main():
    pass


@main.command()
@click.option("--name", help="name of the artifact to build. All artifacts will be built if not specified")
def build(name):
    if name:
        _build_artifact(name)
    else:
        logger.info(f"Artifact name not specified, build all artifacts")
        for artifact_name in ARTIFACTS:
            _build_artifact(artifact_name)


def _build_artifact(name):
    if name not in ARTIFACTS:
        raise ValueError(f"Artifact [{name}] cannot be found, supported: {', '.join(ARTIFACTS.keys())}")

    logger.info(f"Building artifact {name}")
    config = ARTIFACTS[name]
    artifact_type = config['type']
    if artifact_type == "gradle":
        os.system(config['command'])
        os.system(f"cp {config['artifact']} {THEMIS_ARTIFACTS_PATH}/{name}.jar")
    logger.info("Build completed!")


@main.command()
@click.option("--name", help="name of the artifact to clean. All artifacts will be cleaned if not specified")
def clean(name):
    if name:
        _clean_artifact(name)
    else:
        logger.info(f"Artifact name not specified, clean all artifacts")
        for artifact_name in ARTIFACTS:
            _clean_artifact(artifact_name)


def _clean_artifact(name):
    if name not in ARTIFACTS:
        raise ValueError(f"Artifact [{name}] cannot be found, supported: {','.join(ARTIFACTS.keys())}")

    logger.info(f"Cleaning artifact {name}")
    config = ARTIFACTS[name]
    artifact_type = config['type']
    if artifact_type == "gradle":
        os.system(f"rm {THEMIS_ARTIFACTS_PATH}/{name}.jar")
    logger.info("Clean completed!")
