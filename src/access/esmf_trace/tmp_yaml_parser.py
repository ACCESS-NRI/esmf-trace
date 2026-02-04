"""
Copied from https://github.com/ACCESS-NRI/access-experiment-generator/blob/main/src/experiment_generator/tmp_parser/yaml_config.py

A temporary parser for YAML format files.
- `read_yaml`: Load a YAML file into a Python dictionary with preserved quotes.
- `write_yaml`: Dump a Python dictionary back to a YAML file, maintaining format.
"""

import io
import re

import ruamel.yaml

ryaml = ruamel.yaml.YAML()
ryaml.indent(mapping=2, sequence=4, offset=2)
ryaml.preserve_quotes = True
ryaml.width = 10**9  # disable line wrapping for long lines

# remove blank lines between keys:
#   key1: ...
#
#   key2: ...
# ref: https://www.regextester.com/pregsyntax.html
_BLANK_BETWEEN_KEYS = re.compile(
    r"(?m)"  # multi-line mode and don't stop the regex on a line break
    r"^(?P<i>[ \t]*)(?P<k>[^ \t#\-\n][^:\n]*:.*)\n"  # a key line
    r"(?:[ \t]*\n)+"  # one+ blank lines
    r"(?=(?P=i)[^ \t#\-\n][^:\n]*:)"  # next line is a key at same indent
)


def read_yaml(yaml_path: str) -> dict:
    """
    Reads a YAML file and returns a dictionary.
    """
    with yaml_path.open("r", encoding="utf-8") as f:
        return ryaml.load(f)


def write_yaml(data: dict, yaml_path: str) -> None:
    """
    Writes a dictionary to a YAML file while preserving formatting.
    """
    buffer = io.StringIO()
    ryaml.dump(data, buffer)
    txt = buffer.getvalue()
    # collapse only blank lines between sibling mapping entries (all levels)
    txt = _BLANK_BETWEEN_KEYS.sub(r"\g<i>\g<k>\n", txt)
    with yaml_path.open("w", encoding="utf-8") as f:
        f.write(txt)
