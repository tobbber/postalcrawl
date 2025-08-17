from werkzeug.http import parse_options_header


def parse_content_type(content_type: str | None) -> tuple[str, str | None]:
    media_type, options = parse_options_header(content_type)
    charset = options.get("charset")
    if charset:
        charset = charset.lower()
    return media_type, charset
