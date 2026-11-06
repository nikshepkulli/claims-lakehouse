# Container for the claim-denial-risk model (ticket #27). Build context = repository root:
#   docker build -f src/lakehouse/ml/Dockerfile -t claims-denial-risk:local .
# CI does not build this image (structural test only: tests/ml/test_ml_dockerfile.py).
#
# SageMaker runs it as   docker run <image> train   (Training job; reads /opt/ml/input/data/training,
# writes /opt/ml/model) and   docker run <image> score   (Processing job; /opt/ml/processing/*).
# Layout mirrors the repo so lakehouse.features finds governance/*.yaml three levels above src/.
FROM python:3.11.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/program/src

WORKDIR /opt/program

COPY requirements-claimsgen.txt requirements-ml.txt ./
RUN pip install --no-cache-dir -r requirements-ml.txt

COPY src/lakehouse ./src/lakehouse
COPY governance ./governance

# SageMaker mounts /opt/ml itself; run unprivileged and keep the code read-only for that user.
RUN useradd --system --uid 10001 --no-create-home sagemaker
USER 10001

ENTRYPOINT ["python", "-m", "lakehouse.ml"]
CMD ["train"]
