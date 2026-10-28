import gzip
from html.parser import HTMLParser
from pathlib import Path
import re


class Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids = []
        self.assets = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs:
            self.ids.append(attrs['id'])
        for name in ('src', 'href'):
            if attrs.get(name, '').startswith('/static/'):
                self.assets.append(attrs[name].replace('/static/', 'web/', 1))


def main():
    parser = Page()
    parser.feed(Path('web/index.html').read_text(encoding='utf-8'))
    assert len(set(parser.ids)) == len(parser.ids), 'Duplicate HTML IDs'
    for path in parser.assets:
        assert Path(path).is_file(), f'Missing asset: {path}'
    source = Path('web/app.js').read_bytes()
    references = set(re.findall(r"\$\('([^']+)'\)", source.decode()))
    assert not references.difference(parser.ids), f'Missing elements: {references.difference(parser.ids)}'
    compressed = len(gzip.compress(source, mtime=0))
    assert compressed < 200000, 'Checkout JavaScript exceeds the bundle budget'
    print(f'PASS: {len(parser.ids)} unique IDs, {len(parser.assets)} local assets, {len(references)} matching JS element references')
    print(f'Checkout JavaScript: {len(source)} bytes raw / {compressed} bytes gzip (unminified)')


if __name__ == '__main__':
    main()
