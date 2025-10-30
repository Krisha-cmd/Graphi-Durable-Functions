import azure.functions as func
from shared.utils import greet


def main(name: str) -> str:
    # Delegate greeting logic to shared.utils.greet so the function stays thin
    return greet(name)
