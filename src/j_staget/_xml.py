from __future__ import annotations

from lxml import etree

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "prism": "http://prismstandard.org/namespaces/basic/2.0/",
    "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
    "xml": "http://www.w3.org/XML/1998/namespace",
}

def get_texts(entry: etree._Element, xpath_query: str) -> list[str]:
    nodes = entry.xpath(xpath_query, namespaces=NS)
    return [n.text for n in nodes if getattr(n, "text", None)]

def get_first(entry: etree._Element, xpath_query: str):
    vals = get_texts(entry, xpath_query)
    return vals[0] if vals else None

def texts_local(entry: etree._Element, xpath_expr: str) -> list[str]:
    vals = entry.xpath(xpath_expr)
    out: list[str] = []
    for v in vals:
        if isinstance(v, str):
            t = v.strip()
            if t:
                out.append(t)
        else:
            t = getattr(v, "text", None)
            if t and t.strip():
                out.append(t.strip())
    return out

def first_local(entry: etree._Element, xpath_expr: str):
    vals = texts_local(entry, xpath_expr)
    return vals[0] if vals else None

def pick_ja_or_first_tag_local(entry: etree._Element, tag: str) -> str | None:
    ja = first_local(entry, f"./*[local-name()='{tag}']/*[local-name()='ja']/text()")
    if ja:
        return ja
    any_text = first_local(entry, f"./*[local-name()='{tag}']//text()")
    return any_text

def authors_local(entry: etree._Element) -> list[str]:
    ja = texts_local(entry, "./*[local-name()='author']/*[local-name()='ja']/*[local-name()='name']/text()")
    if ja:
        return ja
    return texts_local(entry, "./*[local-name()='author']/*[local-name()='name']/text()")
