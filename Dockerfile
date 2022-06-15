FROM 209229195261.dkr.ecr.us-west-2.amazonaws.com/gdal:latest

USER root

RUN dnf makecache --refresh

RUN dnf install -y --nodocs \
    cmake \
    wget \
    python38-devel \
    gcc \
    gcc-c++


WORKDIR /home
COPY bin/ /home/bin
COPY src/ /home/src
COPY setup.py /home

# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN pip3 install numpy

RUN pip3 install \
    boto3 \
    geojson \
    shapely \
    codeguru_profiler_agent \
    # Dev env
    pytest \
    mock

RUN pip3 install \
    #GDAL==3.4.2
    GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2"."$3}') --global-option=build_ext --global-option="-I/usr/include/gdal"

RUN python3 setup.py install

USER 1000

CMD ["python3", "bin/mr-entry-point.py"]