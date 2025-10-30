def greet(name: str) -> str:
    """Return a greeting for the given name.

    Kept tiny for demo — in your real project this can contain logging,
    sanitization, validation, metrics etc.
    """
    if name is None:
        name = "world"
    return f"Hello {name}!"
