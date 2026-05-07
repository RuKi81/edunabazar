FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps: GDAL, GEOS for PostGIS support + build tools for numpy
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gdal-bin libgdal-dev libgeos-dev \
        gcc g++ gfortran pkg-config libopenblas-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --no-binary numpy numpy>=2.1 \
        -Csetup-args=-Dcpu-baseline=none \
        -Csetup-args=-Dcpu-dispatch=none && \
    pip install --no-cache-dir --no-binary numpy -r requirements.txt gunicorn

COPY . .

RUN DJANGO_DEBUG=1 DJANGO_SECRET_KEY=build-only-key python manage.py collectstatic --noinput

EXPOSE 8000

# Gunicorn: 4 workers × 8 threads = up to 32 concurrent requests.
# We use the gthread worker class because nearly all heavy endpoints
# are I/O-bound (waiting on PostgreSQL on VM2), so threads release the
# GIL and let one worker serve several slow requests in parallel — sync
# workers were making 3-request-bursts knock the site offline.
#
# --timeout 90: hard kill any worker stuck on a single request beyond
# 1.5 min. The previous 300s let a runaway aggregate freeze a worker
# for 5 minutes, multiplying the impact of any slow endpoint.
#
# --max-requests + jitter: recycle a worker after ~1000 requests to
# bound the impact of memory leaks in numpy/PIL raster pipelines.
CMD ["gunicorn", "enb_django.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "4", "--worker-class", "gthread", "--threads", "8", "--timeout", "90", "--graceful-timeout", "30", "--max-requests", "1000", "--max-requests-jitter", "100", "--access-logfile", "-"]
