"""Low-level ER7 (HL7 v2) parsing/building primitives.

Deliberately small and focused: enough structure (delimiters, segments,
fields, components, subcomponents, repetitions, escaping) to support the
laboratory profiles actually required (ORU_R01, ADT_A01 in v2.5.1). This is
NOT a full arbitrary-HL7 parser — that breadth is out of scope. It is also
deliberately separate from the MLLP transport: this layer is pure
message-format handling.
"""
from __future__ import annotations


class HL7Error(ValueError):
    """Raised for malformed ER7 structure."""


class Delimiters:
    __slots__ = ("field", "component", "repeat", "escape", "subcomponent")

    def __init__(self, field, component, repeat, escape, subcomponent):
        self.field = field
        self.component = component
        self.repeat = repeat
        self.escape = escape
        self.subcomponent = subcomponent

    @classmethod
    def default(cls) -> "Delimiters":
        return cls("|", "^", "~", "\\", "&")


def parse_delimiters(msh_segment: str) -> Delimiters:
    """Extract the field separator and encoding characters from an MSH
    segment. MSH is ``MSH<fs><cs><rs><esc><ss>...``."""
    if not msh_segment.startswith("MSH") or len(msh_segment) < 8:
        raise HL7Error("segment is not a well-formed MSH")
    field = msh_segment[3]
    enc = msh_segment[4:8]  # component, repetition, escape, subcomponent
    if len(enc) != 4:
        raise HL7Error("MSH encoding characters must be exactly 4 characters")
    return Delimiters(field, enc[0], enc[1], enc[2], enc[3])


# HL7 escape codecs. Escape must replace the escape char first so the
# produced backslashes are not re-escaped by later steps.
_ESCAPE_PAIRS = (("\\", "\\E\\"), ("|", "\\F\\"), ("^", "\\S\\"), ("&", "\\T\\"), ("~", "\\R\\"))


def escape(text) -> str:
    if text is None:
        return ""
    s = str(text)
    for raw, enc in _ESCAPE_PAIRS:
        s = s.replace(raw, enc)
    return s


def unescape(text):
    if text is None or text == "":
        return text
    s = str(text)
    # unescape backslash-escape LAST so it doesn't re-introduce escapes
    s = s.replace("\\F\\", "|")
    s = s.replace("\\S\\", "^")
    s = s.replace("\\T\\", "&")
    s = s.replace("\\R\\", "~")
    s = s.replace("\\E\\", "\\")
    return s


class Segment:
    __slots__ = ("fields",)

    def __init__(self, fields):
        self.fields = fields or []

    @property
    def name(self) -> str:
        return self.fields[0] if self.fields else ""

    def get(self, idx, default=None):
        return self.fields[idx] if idx < len(self.fields) else default


class Message:
    __slots__ = ("segments", "delims")

    def __init__(self, segments, delims=None):
        self.segments = segments
        self.delims = delims

    def all(self, name) -> list:
        return [s for s in self.segments if s.name == name]

    def first(self, name):
        for s in self.segments:
            if s.name == name:
                return s
        return None


def parse_message(text: str) -> Message:
    """Split a raw ER7 message into segments/fields. Field values remain
    encoded (components/repeats/escapes are decoded by callers)."""
    delims = None
    if "\r" in text:
        raw_segs = text.split("\r")
    elif "\n" in text:
        raw_segs = text.split("\n")
    else:
        raw_segs = [text]

    segments = []
    for raw in raw_segs:
        raw = raw.rstrip("\r\n")
        if not raw.strip():
            continue
        if raw.startswith("MSH"):
            if delims is None:
                delims = parse_delimiters(raw)
            sep = delims.field
        else:
            sep = delims.field if delims is not None else Delimiters.default().field
        segments.append(Segment(raw.split(sep)))

    if delims is None:
        delims = Delimiters.default()
    return Message(segments, delims)


# --- component/repeat/subcomponent helpers -----------------------------------

def component(value, index: int = 0, default="") -> str:
    """Return the ``index``-th component of an encoded field value."""
    if value is None:
        return default
    comps = str(value).split("^")
    return comps[index] if index < len(comps) else default


def split_subcomponents(component_value, sub_sep="&") -> list:
    return str(component_value).split(sub_sep) if component_value else []


def join_field(parts, delim="^") -> str:
    return delim.join(str(p) for p in parts)


def join_repeats(parts, delim="~") -> str:
    return delim.join(str(p) for p in parts)