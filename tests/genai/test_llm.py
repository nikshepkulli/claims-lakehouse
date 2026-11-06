import pytest

from lakehouse.genai.llm import BedrockLLM, FakeLLM, LLMError, extract_sql


def test_extract_sql_variants():
    assert extract_sql("```sql\nSELECT 1\n```") == ("SELECT 1", None)
    assert extract_sql("Sure!\n```\nSELECT 2\n```\nthen DROP TABLE x") == ("SELECT 2", None)
    assert extract_sql("SELECT 3") == ("SELECT 3", None)
    assert extract_sql("REFUSE: no PII") == (None, "no PII")
    assert extract_sql("   ")[0] is None


def test_fake_llm_is_deterministic_and_refuses_unknown():
    llm = FakeLLM({"q": "```sql\nSELECT 1\n```"})
    assert llm.generate("sys", "q") == llm.generate("sys", "q")
    assert llm.generate("sys", "other").text.startswith("REFUSE")
    assert len(llm.calls) == 3


class StubClient:
    """Mimics the documented bedrock-runtime Converse response; records the request."""

    def __init__(self, response=None, exc=None):
        self.response, self.exc, self.request = response, exc, None

    def converse(self, **kw):
        self.request = kw
        if self.exc:
            raise self.exc
        return self.response


GOOD = {
    "output": {"message": {"role": "assistant", "content": [{"text": "```sql\nSELECT 1\n```"}]}},
    "usage": {"inputTokens": 12, "outputTokens": 5},
    "stopReason": "end_turn",
}


def test_bedrock_builds_converse_request_and_parses_response():
    stub = StubClient(GOOD)
    llm = BedrockLLM("model-x", max_tokens=99, client=stub)
    r = llm.generate("SYSTEM", "QUESTION")
    assert (r.text, r.input_tokens, r.output_tokens, r.model_id) == (
        "```sql\nSELECT 1\n```",
        12,
        5,
        "model-x",
    )
    req = stub.request
    assert req["modelId"] == "model-x"
    assert req["system"] == [{"text": "SYSTEM"}]
    assert req["messages"] == [{"role": "user", "content": [{"text": "QUESTION"}]}]
    assert req["inferenceConfig"] == {"maxTokens": 99, "temperature": 0.0}


@pytest.mark.parametrize(
    "stub",
    [
        StubClient({"unexpected": 1}),
        StubClient(exc=RuntimeError("throttled")),
        StubClient({"output": {"message": {"content": [{"text": " "}]}}}),
    ],
)
def test_bedrock_errors_become_llm_error(stub):
    with pytest.raises(LLMError):
        BedrockLLM(client=stub).generate("s", "q")


def test_bedrock_region_comes_from_env_only(monkeypatch):
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    with pytest.raises(RuntimeError, match="AWS_REGION"):
        BedrockLLM().client  # noqa: B018 - property access builds the boto3 client


def test_bedrock_creates_boto3_client_with_env_region(monkeypatch):
    boto3 = pytest.importorskip("boto3")
    created = {}
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setattr(
        boto3, "client", lambda svc, region_name: created.update(svc=svc, r=region_name) or object()
    )
    BedrockLLM().client  # noqa: B018
    assert created == {"svc": "bedrock-runtime", "r": "eu-west-1"}
