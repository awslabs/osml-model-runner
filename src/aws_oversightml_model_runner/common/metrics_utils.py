from aws_embedded_metrics.config import get_config


def build_embedded_metrics_config():
    metrics_config = get_config()
    metrics_config.service_name = "AWSOversightML"
    metrics_config.log_group_name = "/aws/OversightML/ModelRunner"
    metrics_config.namespace = "AWSOversightML"
    metrics_config.environment = "local"
