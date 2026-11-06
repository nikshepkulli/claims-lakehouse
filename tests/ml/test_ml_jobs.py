import pytest

from lakehouse.ml import jobs

ARN = "arn:aws:iam::111122223333:role/r"
KMS = "arn:aws:kms:eu-west-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab"
IMG = "111122223333.dkr.ecr.eu-west-1.amazonaws.com/claims-lakehouse/claim-denial-risk:v1"


def _train():
    return jobs.training_job_request(
        job_name="denial-train-1",
        role_arn=ARN,
        image_uri=IMG,
        features_s3_uri="s3://gold/features/",
        output_s3_uri="s3://ml/models/",
        kms_key_arn=KMS,
        hyperparameters={"seed": 3, "include_quasi": False},
    )


def _score():
    return jobs.scoring_job_request(
        job_name="denial-score-1",
        role_arn=ARN,
        image_uri=IMG,
        features_s3_uri="s3://gold/features/",
        model_s3_uri="s3://ml/models/denial-train-1/output/",
        scores_s3_uri="s3://gold/scores/claim_denial_risk/",
        kms_key_arn=KMS,
    )


def _register():
    return jobs.model_package_request(
        group_name="claim-denial-risk",
        image_uri=IMG,
        model_data_s3_uri="s3://ml/models/denial-train-1/output/model.tar.gz",
        metrics_s3_uri="s3://ml/training-output/denial-train-1/metrics.json",
        model_version="v0123456789ab",
        feature_manifest_sha256="a" * 64,
    )


def test_training_request_is_bounded_private_and_encrypted():
    r = _train()
    assert r["HyperParameters"] == {"seed": "3", "include_quasi": "False"}  # all strings
    assert r["ResourceConfig"]["InstanceCount"] == 1
    assert r["ResourceConfig"]["InstanceType"].startswith("ml.m5.")
    assert r["StoppingCondition"]["MaxRuntimeInSeconds"] <= 3600
    assert r["EnableNetworkIsolation"] is True
    assert r["OutputDataConfig"]["KmsKeyId"] == KMS and r["ResourceConfig"]["VolumeKmsKeyId"] == KMS
    assert r["InputDataConfig"][0]["ChannelName"] == "training"
    assert "EnableManagedSpotTraining" not in r


def test_metric_definitions_match_what_the_trainer_prints(trained):
    import re

    from lakehouse.ml.train import sagemaker_metric_lines

    lines = sagemaker_metric_lines(trained[1])
    for d in jobs.METRIC_DEFINITIONS:
        hits = [m for ln in lines if (m := re.search(d["Regex"], ln))]
        assert len(hits) == 1 and 0 <= float(hits[0].group(1)) <= 1


def test_scoring_request_uses_the_cli_default_processing_layout():
    from lakehouse.ml import cli

    r = _score()
    locals_ = {i["InputName"]: i["S3Input"]["LocalPath"] for i in r["ProcessingInputs"]}
    assert locals_["features"] == str(cli.PROCESSING / "input" / "features")
    assert locals_["model"] == str(cli.PROCESSING / "input" / "model")
    out = r["ProcessingOutputConfig"]["Outputs"][0]["S3Output"]
    assert out["LocalPath"] == str(cli.PROCESSING / "output" / "scores")
    assert r["AppSpecification"]["ContainerArguments"] == ["score"]
    assert r["NetworkConfig"]["EnableNetworkIsolation"] is True


def test_registered_version_waits_for_manual_approval():
    r = _register()
    assert r["ModelApprovalStatus"] == "PendingManualApproval"
    assert r["CustomerMetadataProperties"]["data_is_synthetic"] == "true"


CASES = [
    (_train, "CreateTrainingJob"),
    (_score, "CreateProcessingJob"),
    (_register, "CreateModelPackage"),
]


@pytest.mark.parametrize("build,op", CASES)
def test_requests_validate_against_botocores_sagemaker_api_model(build, op):
    """Offline shape check (no AWS call). Skipped where botocore is not installed (CI)."""
    botocore = pytest.importorskip("botocore")
    from botocore.session import get_session
    from botocore.validate import validate_parameters

    model = get_session().get_service_model("sagemaker")
    shape = model.operation_model(op).input_shape
    report = validate_parameters(build(), shape)  # raises ParamValidationError if malformed
    assert report is None
    bad = {**build(), "Tags": "not-a-list"}
    with pytest.raises(botocore.exceptions.ParamValidationError):
        validate_parameters(bad, shape)  # the validator really rejects malformed requests
