def greet(name: str) -> str:
    """Return a greeting for the given name.

    Kept tiny for demo — in your real project this can contain logging,
    sanitization, validation, metrics etc.
    """
    if name is None:
        name = "world"
    return f"Hello {name}!"


def normalize_doi(doi: str) -> str:
    """Normalize a DOI by stripping any leading URL (up to .org/) and
    replacing slashes so it can be used as a key/id."""
    if not doi:
        return ""
    d = doi.strip()
    if ".org/" in d:
        d = d.split('.org/')[-1]
    # replace slashes with underscores for safe keys
    return d.replace('/', '_')
