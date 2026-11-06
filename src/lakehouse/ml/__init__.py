"""SageMaker-ready claim-denial-risk model (ticket #27).

Training, calibration, evaluation, batch scoring and the SageMaker container contract, all
runnable locally with scikit-learn (no AWS calls anywhere). See docs/ml.md and docs/model-card.md.
The data is 100% synthetic, so no metric here says anything about real-world performance.
"""

MODEL_NAME = "claim_denial_risk"
