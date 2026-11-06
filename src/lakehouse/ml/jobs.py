"""SageMaker API request bodies for the three cloud steps: train, register, batch-score.

Pure functions that build the dicts boto3 would send (``create_training_job``,
``create_model_package``, ``create_processing_job``). Nothing here calls AWS unless ``submit`` is
used, which imports boto3 lazily and needs credentials. **The author has never submitted these to
a real account**; tests check the shapes offline (and against botocore's API model when botocore
is installed). Defaults are deliberately small and bounded: one CPU instance, a hard runtime
limit, network isolation for training, KMS on volumes and outputs, tags.
"""

from __future__ import annotations

from . import MODEL_NAME

TRAIN_INPUT = "/opt/ml/input/data/training"
METRIC_DEFINITIONS = [
    {"Name": f"test:{m}", "Regex": rf"test:{m}=([0-9.eE+-]+);"}
    for m in ("auc_pr", "roc_auc", "brier", "ece")
]


def _tags(extra: dict | None) -> list[dict]:
    base = {"Project": "claims-lakehouse", "Component": "sagemaker-ml", "DataClass": "synthetic"}
    return [{"Key": k, "Value": v} for k, v in {**base, **(extra or {})}.items()]


def training_job_request(
    *,
    job_name: str,
    role_arn: str,
    image_uri: str,
    features_s3_uri: str,
    output_s3_uri: str,
    kms_key_arn: str,
    hyperparameters: dict | None = None,
    instance_type: str = "ml.m5.large",
    volume_gb: int = 10,
    max_runtime_s: int = 3600,
    vpc_config: dict | None = None,
    tags: dict | None = None,
) -> dict:
    req = {
        "TrainingJobName": job_name,
        "RoleArn": role_arn,
        "AlgorithmSpecification": {
            "TrainingImage": image_uri,
            "TrainingInputMode": "File",
            "MetricDefinitions": METRIC_DEFINITIONS,
        },
        "HyperParameters": {k: str(v) for k, v in (hyperparameters or {}).items()},
        "InputDataConfig": [
            {
                "ChannelName": "training",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": features_s3_uri,
                        "S3DataDistributionType": "FullyReplicated",
                    }
                },
            }
        ],
        "OutputDataConfig": {"S3OutputPath": output_s3_uri, "KmsKeyId": kms_key_arn},
        "ResourceConfig": {
            "InstanceType": instance_type,
            "InstanceCount": 1,
            "VolumeSizeInGB": volume_gb,
            "VolumeKmsKeyId": kms_key_arn,
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": max_runtime_s},
        "EnableNetworkIsolation": True,
        "EnableInterContainerTrafficEncryption": True,
        "Tags": _tags(tags),
    }
    if vpc_config:
        req["VpcConfig"] = vpc_config
    return req


def scoring_job_request(
    *,
    job_name: str,
    role_arn: str,
    image_uri: str,
    features_s3_uri: str,
    model_s3_uri: str,
    scores_s3_uri: str,
    kms_key_arn: str,
    instance_type: str = "ml.m5.large",
    volume_gb: int = 10,
    max_runtime_s: int = 1800,
    tags: dict | None = None,
) -> dict:
    """Batch scoring as a Processing job running ``python -m lakehouse.ml score`` (the image's
    entrypoint) against the /opt/ml/processing layout the CLI defaults to."""

    def inp(name, uri, dest):
        return {
            "InputName": name,
            "S3Input": {
                "S3Uri": uri,
                "LocalPath": dest,
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
            },
        }

    return {
        "ProcessingJobName": job_name,
        "RoleArn": role_arn,
        "AppSpecification": {"ImageUri": image_uri, "ContainerArguments": ["score"]},
        "ProcessingInputs": [
            inp("features", features_s3_uri, "/opt/ml/processing/input/features"),
            inp("model", model_s3_uri, "/opt/ml/processing/input/model"),
        ],
        "ProcessingOutputConfig": {
            "Outputs": [
                {
                    "OutputName": "scores",
                    "S3Output": {
                        "S3Uri": scores_s3_uri,
                        "LocalPath": "/opt/ml/processing/output/scores",
                        "S3UploadMode": "EndOfJob",
                    },
                }
            ],
            "KmsKeyId": kms_key_arn,
        },
        "ProcessingResources": {
            "ClusterConfig": {
                "InstanceCount": 1,
                "InstanceType": instance_type,
                "VolumeSizeInGB": volume_gb,
                "VolumeKmsKeyId": kms_key_arn,
            }
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": max_runtime_s},
        "NetworkConfig": {"EnableNetworkIsolation": True},
        "Tags": _tags(tags),
    }


def model_package_request(
    *,
    group_name: str,
    image_uri: str,
    model_data_s3_uri: str,
    metrics_s3_uri: str,
    model_version: str,
    feature_manifest_sha256: str,
) -> dict:
    """Registry step: a new version in the Model Package Group, ``PendingManualApproval``.

    Lineage/approval only: this repo ships no serving entrypoint, so the package must not be
    deployed to an endpoint as is (docs/ml.md, "Not built").
    """
    return {
        "ModelPackageGroupName": group_name,
        "ModelPackageDescription": f"{MODEL_NAME} {model_version} (synthetic data)",
        "InferenceSpecification": {
            "Containers": [{"Image": image_uri, "ModelDataUrl": model_data_s3_uri}],
            "SupportedContentTypes": ["text/csv", "application/json"],
            "SupportedResponseMIMETypes": ["application/json", "text/csv"],
        },
        "ModelMetrics": {
            "ModelQuality": {
                "Statistics": {"ContentType": "application/json", "S3Uri": metrics_s3_uri}
            }
        },
        "ModelApprovalStatus": "PendingManualApproval",
        "CustomerMetadataProperties": {
            "model_version": model_version,
            "feature_manifest_sha256": feature_manifest_sha256,
            "data_is_synthetic": "true",
        },
    }


def submit(kind: str, request: dict, region: str | None = None):  # pragma: no cover - needs AWS
    """Send one request to AWS. Needs boto3 and credentials; never called by tests or CI."""
    import boto3

    client = boto3.client("sagemaker", region_name=region)
    call = {
        "training": client.create_training_job,
        "scoring": client.create_processing_job,
        "register": client.create_model_package,
    }[kind]
    return call(**request)
