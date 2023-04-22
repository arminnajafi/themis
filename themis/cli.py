import click
import logging
import themis.spark.emr as emr

logger = logging.getLogger(__name__)


@click.group()
def main():
    pass


@main.command()
@click.option('-e', '--engine', type=str, default="spark-emr-ec2", help="type of engine")
@click.option('-t', '--template', type=str, default="sf", help="specific template for creating the engine")
@click.option('-c', '--context', type=(str, str), multiple=True, help="context to fill for a template")
def init(engine, template, context):
    context_dict = dict(context)
    logger.info(f"Initializing engine: {engine}, template: {template}, context: {context_dict}")

    if engine == "spark-emr-ec2":
        if template == "sf":
            emr.create_emr_ec2_sf_cluster(**context_dict)
        elif template == "dpu":
            emr.create_emr_ec2_dpu_cluster(**context_dict)
        else:
            raise ValueError(f"Unsupported template: [{template}], supported: [sf, dpu]")
    else:
        raise ValueError(f"Unsupported engine: [{engine}], supported: [spark-emr-ec2]")
    logger.info(f"Completed initialization")