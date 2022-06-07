class MetricsConfig:
    def __init__(self, namespace: str = None, output_type: str = "stdout"):
        self.namespace = namespace
        self.output_type = output_type
