#!/bin/bash

# Script for installing GDAL on an AL2 dev desktop from source using tar files in the /assets directory.
# Run the script from the root level (AWSOversightMLModelRunner) as root
# sudo ./scripts/bash/install_gdal_local_al2.sh

# For provisioning and compilation
PACKAGES="python3-devel python3-pip wget unzip make automake libtool gcc gcc-c++ sqlite sqlite-devel.x86_64 cmake openssl libtool.x86_64 gcc10-binutils.x86_64"

# For PROJ and GDAL
GDAL_PKGS="libcurl-devel libtiff-devel zlib-devel libzstd libjpeg-turbo-devel libpng-devel libwebp-devel expat-devel postgresql-devel"

# Update and clean yum, then install all required packages
yum update -y
yum clean all
yum install -y amazon-linux-extras
yum install -y $PACKAGES $GDAL_PKGS


echo "Build OpenJPEG"
OPENJPEG_VERSION=2.3.1
OPENJPEG_FILE=open-jpeg-$OPENJPEG_VERSION.tar.gz
OPENJPEG_SOURCE=assets/$OPENJPEG_FILE
OPENJPEG_DEST=/tmp/$OPENJPEG_FILE
cp $OPENJPEG_SOURCE $OPENJPEG_DEST
chmod -R 755 $OPENJPEG_DEST
tar xzf $OPENJPEG_DEST
rm -f $OPENJPEG_DEST
cd openjpeg-$OPENJPEG_VERSION
cmake . -DBUILD_SHARED_LIBS=ON  -DBUILD_STATIC_LIBS=OFF -DCMAKE_BUILD_TYPE=Release
make -j $(nproc)
make install
cd ..
rm -rf openjpeg-$OPENJPEG_VERSION


echo "Build PROJ"
PROJ_VERSION=6.1.1
PROJ_FILE=proj-$PROJ_VERSION.tar.gz
PROJ_SOURCE=assets/$PROJ_FILE
PROJ_DEST=/tmp/$PROJ_FILE
PROJ_AUTOGEN_FILE=autogen.sh
PROJ_AUTOGEN_SOURCE=scripts/bash/$PROJ_AUTOGEN_FILE
PROJ_AUTOGEN_DEST=proj/$PROJ_AUTOGEN_FILE

cp $PROJ_SOURCE $PROJ_DEST
chmod -R 755 $PROJ_DEST

mkdir proj
cp $PROJ_AUTOGEN_SOURCE $PROJ_AUTOGEN_DEST
chmod 755 $PROJ_AUTOGEN_DEST
tar -C proj --strip-components=1 -xf $PROJ_DEST

cd proj
bash $PROJ_AUTOGEN_FILE
./configure --disable-static
make -j $(nproc)
make install
cd ..
rm -rf proj

echo "  Build PROJ_DATUMGRID"
PROJ_DATUMGRID_VERSION=1.8
PROJ_DATUMGRID_FILE=proj-datumgrid-$PROJ_DATUMGRID_VERSION.zip
PROJ_DATUMGRID_SOURCE=assets/$PROJ_DATUMGRID_FILE
PROJ_DATUMGRID_DEST=/tmp/$PROJ_DATUMGRID_FILE
cp $PROJ_DATUMGRID_SOURCE $PROJ_DATUMGRID_DEST
chmod -R 755 $PROJ_DATUMGRID_DEST
unzip -q -j -u -o $PROJ_DATUMGRID_DEST -d /usr/local/share/proj
rm -f $PROJ_DATUMGRID_DEST


echo "Build GDAL"
GDAL_VERSION=3.5.1
GDAL_FILE=gdal-$GDAL_VERSION.tar.gz
GDAL_SOURCE=assets/$GDAL_FILE
GDAL_DEST=/tmp/$GDAL_FILE

cp $GDAL_SOURCE $GDAL_DEST
chmod -R 755 $GDAL_DEST
mkdir gdal
tar -C gdal --strip-components=1 -xf $GDAL_DEST
cd gdal
./configure --without-libtool --with-hide-internal-symbols --with-libtiff=internal \
   --with-rename-internal-libtiff-symbols --with-geotiff=internal --with-rename-internal-libgeotiff-symbols \
   --with-fgdb=$FILEGDB_INSTALL --with-openjpeg --with-python
make -j $(nproc)
make install
cd ..
rm -rf gdal

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/usr/local/lib:/usr/include"
pip3 install gdal==$(gdal-config --version | awk -F'[.]' '{print $1"."$2"."$3}')

echo "NOTE: you may need to set the env variable 'export LD_LIBRARY_PATH='/usr/local/lib':\$LD_LIBRARY_PATH' in your .bashrc"
