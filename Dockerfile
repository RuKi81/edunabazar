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

CMD ["gunicorn", "enb_django.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "3", "--timeout", "300"]
