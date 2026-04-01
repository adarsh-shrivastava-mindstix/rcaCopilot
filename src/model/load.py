from langchain_aws import ChatBedrock

# Uses US inference profile for Claude Opus 4.6
# Update this string if your account exposes a different exact model id revision.
MODEL_ID = "us.anthropic.claude-opus-4-6-v1"

def load_model() -> ChatBedrock:
    """
    Get Bedrock model client.
    Uses IAM authentication via the execution role.
    """
    return ChatBedrock(model_id=MODEL_ID)
