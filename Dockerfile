FROM python:3.10
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
RUN echo 'deb http://archive.debian.org/debian stretch main contrib non-free' >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get autoremove -y && \
    apt-get install -y libssl-dev curl git nano wget && \
    apt-get install -y postgresql-server-dev-all gdal-bin python3-gdal libgeos-dev libproj-dev && \
    rm -rf /var/lib/apt/lists/* && rm -rf /var/lib/apt/lists/partial/*

# Clean up
RUN rm -rf /var/lib/apt/lists/* && rm -rf /var/lib/apt/lists/partial/*

WORKDIR /code
COPY . .

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
