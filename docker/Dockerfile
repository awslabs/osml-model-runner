# Use GDAL_BASE_IMG from GitLab CI variables
ARG BASE_IMG
#checkov:skip=CKV_DOCKER_7:This is using an already built non latest image that we maintain
FROM ${BASE_IMG} as runner

##### ##### ##### ##### ##### ##### ##### ##### ##### #####
# Configure Mirrors
##### ##### ##### ##### #####
# Pip Mirrors
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOST=pypi.org
#checkov:skip=CKV2_DOCKER_16: PIP_TRUSTED_HOST used for accessing approved mirrors.

ENV PIP_INDEX_URL=${PIP_INDEX_URL}
ENV PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}
#checkov:skip=CKV2_DOCKER_16: PIP_TRUSTED_HOST used for accessing approved mirrors.

ARG PYTHON_BINARY=python3.13
ARG PIP_BINARY=pip3.13

ENV PYTHON_BINARY=${PYTHON_BINARY}

# Justification: we switch to a non-root user before running the container in any capacity,
# but a parsing error prevents semgrep from detecting that.
# We pass this same check in checkov.
# nosemgrep: dockerfile.security.last-user-is-root.last-user-is-root
USER root

# Skip package installation - gdal-base already includes required packages
# RUN dnf update -y \
#     && dnf install -y --setopt=tsflags=nodocs --allowerasing $PACKAGES  \
#     && rm -f /usr/lib/libturbojpeg.so* \
#     && rm -f /usr/lib/libwebpmux.so* /usr/lib/libwebpdemux.so* /usr/lib/libwebpdecoder.so* \
#     && dnf clean all \
#     && rm -rf /var/cache/dnf/ /var/tmp/* /tmp/* /var/tmp/.???* /tmp/.???*

WORKDIR /home

# copy our local application source into the container
COPY . osml-model-runner

# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Upgrade pip
RUN ${PIP_BINARY} install \
    --no-cache-dir \
    --index-url ${PIP_INDEX_URL} \
    --trusted-host ${PIP_TRUSTED_HOST} \
    pip -U

# install the model runner application from source
RUN ${PYTHON_BINARY} -m pip install \
    --no-cache-dir \
    --index-url ${PIP_INDEX_URL} \
    --trusted-host ${PIP_TRUSTED_HOST} \
    osml-model-runner/.

# Resolve broken simlinks for gdal python header bindings after python installs.
RUN ldconfig
# Clean up old python installs
USER root
RUN rm -rf /usr/lib/python3.6/site-packages/urllib3* \
           /usr/lib/python3.9 \
           /home/osml-model-runner/.venv/lib/python3.11 \
           /usr/share/python3-wheels/setuptools-*.whl \
           /usr/share/python3.11-wheels

# Create directory that pip can install required packages
# for unit tests to run. Needed for pipeline unit-test job.
RUN mkdir -p /.local/lib
RUN chown -R 1000:1000 /.local
RUN chmod -R 744 /.local

# Create entrypoint script for variable support (as root)
RUN echo '#!/bin/bash' > /entrypoint.sh && \
    echo "exec ${PYTHON_BINARY} osml-model-runner/bin/oversightml-mr-entry-point.py \"\$@\"" >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

USER 1000

ENV CODEGURU_PROFILING_GROUP=mr-profiler

ENTRYPOINT ["/entrypoint.sh"]

FROM runner as unit-test

WORKDIR /home/osml-model-runner

USER root

# Pyton packages and environment variables mirror tox.ini settings
ENV AWS_DEFAULT_REGION=us-west-2
ENV WORKERS=4
ENV WORKERS_PER_CPU=1
ENV JOB_TABLE=TEST-JOB-TABLE
ENV ENDPOINT_TABLE=TEST-ENDPOINT-STATS-TABLE
ENV FEATURE_TABLE=TEST-FEATURE-TABLE
ENV REGION_REQUEST_TABLE=TEST-REGION-REQUEST-TABLE
ENV TILE_REQUEST_TABLE=TEST-TILE-REQUEST-TABLE
ENV IMAGE_QUEUE=TEST-IMAGE-QUEUE
ENV REGION_QUEUE=TEST-REGION-QUEUE
ENV IMAGE_STATUS_TOPIC=TEST-IMAGE-STATUS-TOPIC
ENV REGION_STATUS_TOPIC=TEST-REGION-STATUS-TOPIC
ENV SM_SELF_THROTTLING=true
# MOTO/BOTO
ENV AWS_ACCESS_KEY_ID=testing
ENV AWS_SECRET_ACCESS_KEY=testing
ENV AWS_SECURITY_TOKEN=testing
ENV AWS_SESSION_TOKEN=testing

RUN ${PYTHON_BINARY} -m pip install \
    --no-cache-dir \
    --index-url ${PIP_INDEX_URL} \
    --trusted-host ${PIP_TRUSTED_HOST} \
    -r /home/osml-model-runner/test/requirements-test.txt


RUN chown -R 1000:1000 /home/osml-model-runner
RUN chmod -R 744 /home/osml-model-runner

# Create entrypoint script for unit tests (as root)
RUN echo '#!/bin/bash' > /test-entrypoint.sh && \
    echo "exec ${PYTHON_BINARY} -m pytest \"\$@\"" >> /test-entrypoint.sh && \
    chmod +x /test-entrypoint.sh

USER 1000

ENTRYPOINT ["/test-entrypoint.sh"]
CMD ["--durations=10", "--cov-config", ".coveragerc", "--cov", "aws.osml.model_runner"] 
#checkov:skip=CKV_DOCKER_2:HEALTHCHECK to be added later.
