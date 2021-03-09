FROM oversight-base:latest AS base

RUN python3 -m pip install --upgrade pip && python3 -m pip install \
    geojson

WORKDIR /processing-service

###############################################################################
# Model Runner Code Baseline
###############################################################################
COPY ./oversightmr ./

CMD ["sh", "-c", "python3 model_runner.py -iq ${image_queue} -ft ${feature_table} -jt ${job_table} -wpc ${workers_per_cpu}"]