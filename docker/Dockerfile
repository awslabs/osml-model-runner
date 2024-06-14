#TODO: Update BASE_IMG to be the DSO built base gitlabs 
ARG BASE_IMG=registry.gitlab.aws.dev/helix/aip/gdal-base:latest
#checkov:skip=CKV_DOCKER_7:This is using an already built non latest image that we maintain
FROM ${BASE_IMG} as runner
# Only override if you're using a mirror with a cert pulled in using cert-base as a build parameter
ARG PIP_INDEX_URL=https://pypi.org/simple/
ARG PACKAGES="cmake wget gcc gcc-c++ libstdc++ sqlite-libs libcurl libtiff zlib libzstd libjpeg-turbo libpng openjpeg2 libwebp expat libpq"

# Justification: we switch to a non-root user before running the container in any capacity,
# but a parsing error prevents semgrep from detecting that.
# We pass this same check in checkov.
# nosemgrep: dockerfile.security.last-user-is-root.last-user-is-root
USER root
RUN dnf update -y \
    && dnf install -y --setopt=tsflags=nodocs --allowerasing $PACKAGES  \
    # libturbojpeg.so is not used by GDAL. Only libjpeg.so*, I don't think this exists here though
    && rm -f /usr/lib/libturbojpeg.so* \
    # Only libwebp.so is used by GDAL
    && rm -f /usr/lib/libwebpmux.so* /usr/lib/libwebpdemux.so* /usr/lib/libwebpdecoder.so* \
    && dnf clean all \
    && rm -rf /var/cache/dnf/ /var/tmp/* /tmp/* /var/tmp/.???* /tmp/.???*

WORKDIR /home

# copy our lcoal application source into the container
COPY . osml-model-runner

# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Upgrade pip 
RUN pip3 install \    
    --no-cache-dir \
    --index-url ${PIP_INDEX_URL} \ 
    --trusted-host https://mirrors.gs.mil \
    pip -U 

# install the model runner application from source
RUN python3 -m pip install \
    --no-cache-dir \
    --index-url ${PIP_INDEX_URL} \ 
    --trusted-host https://mirrors.gs.mil \
    osml-model-runner/.

# Resolve broken simlinks for gdal python header bindings after python installs. 
RUN ldconfig 
# Clean up old python3.6 install
USER root
RUN rm -rf /usr/lib/python3.6/site-packages/urllib3*

# Create directory that pip can install required packages
# for unit tests to run. Needed for pipeline unit-test job.
RUN mkdir -p /.local/lib
RUN chown -R 1000:1000 /.local
RUN chmod -R 744 /.local

USER 1000

ENTRYPOINT ["python3", "osml-model-runner/bin/oversightml-mr-entry-point.py"]

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
ENV IMAGE_QUEUE=TEST-IMAGE-QUEUE
ENV REGION_QUEUE=TEST-REGION-QUEUE
ENV IMAGE_STATUS_TOPIC=TEST-IMAGE-STATUS-TOPIC
ENV SM_SELF_THROTTLING=true
# MOTO/BOTO
ENV AWS_ACCESS_KEY_ID=testing
ENV AWS_SECRET_ACCESS_KEY=testing
ENV AWS_SECURITY_TOKEN=testing
ENV AWS_SESSION_TOKEN=testing

RUN python3 -m pip install \
    --no-cache-dir \
    --index-url ${PIP_INDEX_URL} \ 
    --trusted-host https://mirrors.gs.mil \
    -r /home/osml-model-runner/test/requirements-test.txt


RUN chown -R 1000:1000 /home/osml-model-runner
RUN chmod -R 744 /home/osml-model-runner

USER 1000
ENTRYPOINT ["pytest"]
CMD ["--durations=10", "--cov-config", ".coveragerc", "--cov", "aws.osml.model_runner"] 
#checkov:skip=CKV_DOCKER_2:HEALTHCHECK to be added later.