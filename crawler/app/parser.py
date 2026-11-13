"""HTML parsing: page title from <title>, citations from <a class="citation">.

Stdlib-only so the crawler image stays small and the parsing rules are exactly
the ones specified (class 'citation' may appear among other classes).
"""

from html.parser import HTMLParser


class _CitationParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._in_title = False
        self._title_parts = []
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "title":
            self._in_title = True
        elif tag == "a":
            attr = dict(attrs)
            classes = (attr.get("class") or "").split()
            if "citation" in classes and attr.get("href"):
                self.links.append(attr["href"].strip())

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False

    def handle_data(self, data):
        if self._in_title:
            self._title_parts.append(data)


def parse_html(text: str):
    """Return (title, links): title text (or None) and de-duplicated hrefs,
    in document order."""
    parser = _CitationParser()
    parser.feed(text)
    parser.close()
    title = " ".join("".join(parser._title_parts).split()) or None
    seen, links = set(), []
    for href in parser.links:
        if href and href not in seen:
            seen.add(href)
            links.append(href)
    return title, links
