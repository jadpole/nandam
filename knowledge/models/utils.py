import unicodedata


def shorten_description(description: str | None) -> str | None:
    if not description:
        return None

    words = (
        unicodedata.normalize("NFKD", description)
        .encode("ASCII", "ignore")
        .decode()
        .split("{noformat}", 1)[0]
        .split("```", 1)[0]
        .split()
    )
    if not words:
        return None
    elif len(words) > 50:  # noqa: PLR2004
        return " ".join(words[:50]) + "..."
    else:
        return " ".join(words)
