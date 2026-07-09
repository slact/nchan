# Production Nchan image: OpenResty + nchan, with -DNDEBUG to prevent
# worker core dumps from spool_fetch_msg assertion failures.
#
# Based on the SAME OpenResty version as the original lloydzhou/nchan image,
# ensuring full compatibility with nchan_publisher_upstream_request (which
# did NOT work correctly with plain nginx).
#
# Build:
#   docker build -t lloydzhou/nchan:latest .

FROM openresty/openresty:1.25.3.1-jammy

USER root

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential libpcre3-dev libssl-dev zlib1g-dev wget perl \
    && rm -rf /var/lib/apt/lists/*

# Download OpenResty source (same version as the base image)
RUN cd /tmp && \
    wget -q https://openresty.org/download/openresty-1.25.3.1.tar.gz && \
    tar xzf openresty-1.25.3.1.tar.gz && \
    rm openresty-1.25.3.1.tar.gz

# Copy nchan source (includes lloyd's stub_status multi-format patch)
COPY . /nchan

WORKDIR /tmp/openresty-1.25.3.1

# Build OpenResty + nchan.
# -DNDEBUG disables C assert() to prevent worker core dumps.
RUN ./configure \
    --prefix=/usr/local/openresty \
    --add-module=/nchan \
    --with-cc-opt="-DNDEBUG" \
    -j$(nproc) && \
    make -j$(nproc) && make install

# Cleanup build artifacts
RUN rm -rf /tmp/openresty-1.25.3.1 /nchan && \
    apt-get purge -y build-essential libpcre3-dev libssl-dev zlib1g-dev wget perl && \
    apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# OpenResty paths (same as original image):
#   binary: /usr/local/openresty/bin/openresty
#   conf:   /usr/local/openresty/nginx/conf/
#   modules:/usr/local/openresty/nginx/modules/
EXPOSE 80

CMD ["/usr/local/openresty/bin/openresty", "-g", "daemon off;"]
