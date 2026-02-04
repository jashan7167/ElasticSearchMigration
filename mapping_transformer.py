"""
mapping_transformer.py
Converts Elasticsearch 6.x mappings  →  8.x-compatible mappings.

Key changes handled
───────────────────
1.  Mapping types removed   – 6.x wraps props under a type key
                               (e.g. {"my_type": {"properties": …}})
                               8.x expects       {"properties": …}
2.  "type": "object" fields – kept as-is (still valid in 8.x)
3.  Deprecated field types  – some 6.x types need renaming
4.  Index settings cleanup  – removes 6.x-only settings that 8.x rejects
"""

import copy
import json
from typing import Any, Dict, List, Tuple

from logger import get_logger

logger = get_logger("mapping_transformer")

# ── 6.x field-type  →  8.x field-type renames ───────────────────────
DEPRECATED_TYPE_MAP: Dict[str, str] = {
    # 6.x "string" was already removed in 5.x, but some custom mappings
    # may still slip through; map to "text" as the safe default.
    "string": "text",
}

# ── index settings that are invalid / deprecated in 8.x ─────────────
REMOVED_SETTINGS: List[str] = [
    "index.mapper.dynamic",
    "index.mapping.total_fields.limit",  # still exists but may need tuning
]

# Read-only / auto-managed settings that cannot be set on index creation
AUTO_MANAGED_SETTINGS: List[str] = [
    "creation_date",
    "uuid", 
    "version",
    "provided_name",
    "version.created",
    "version.upgraded",
]


def _strip_type_wrapper(raw_mapping: Dict[str, Any]) -> Dict[str, Any]:
    """
    6.x raw mapping looks like:
        { "my_type": { "properties": { … } } }
    8.x expects:
        { "properties": { … } }

    If the mapping already has "properties" at the top level (no type
    wrapper) we return it unchanged.
    """
    # Already unwrapped / 7.x+ style
    if "properties" in raw_mapping:
        return raw_mapping

    # Find the single type key and unwrap
    keys = [k for k in raw_mapping if k != "_source"]
    if len(keys) == 1:
        inner = raw_mapping[keys[0]]
        if isinstance(inner, dict):
            # preserve _source if present at top level
            if "_source" in raw_mapping:
                inner["_source"] = raw_mapping["_source"]
            logger.debug("Unwrapped mapping type '%s'", keys[0])
            return inner

    # Multiple types – 8.x does NOT support this.
    # We flatten into a single mapping (last-writer-wins for conflicts).
    logger.warning(
        "Multiple mapping types detected (%s). "
        "Flattening into single mapping – review manually.", keys
    )
    merged: Dict[str, Any] = {}
    for key in keys:
        if isinstance(raw_mapping[key], dict):
            props = raw_mapping[key].get("properties", {})
            merged.update(props)
    return {"properties": merged}


def _rename_deprecated_types(mapping: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively walk properties and rename deprecated field types."""
    mapping = copy.deepcopy(mapping)
    props   = mapping.get("properties", {})
    for field_name, field_def in props.items():
        if not isinstance(field_def, dict):
            continue
        # rename if needed
        old_type = field_def.get("type")
        if old_type in DEPRECATED_TYPE_MAP:
            field_def["type"] = DEPRECATED_TYPE_MAP[old_type]
            logger.info("Renamed field '%s' type: %s → %s",
                        field_name, old_type, field_def["type"])
        # recurse into nested / object properties
        if "properties" in field_def:
            field_def = _rename_deprecated_types(field_def)
            props[field_name] = field_def
    mapping["properties"] = props
    return mapping


def clean_settings(raw_settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Strip out settings that 8.x will reject.
    raw_settings is the value under index.<name>.settings.index
    """
    settings = copy.deepcopy(raw_settings)
    # The raw response nests under  { "index_name": { "settings": { "index": { … } } } }
    # Normalise to just the inner dict
    if len(settings) == 1:
        key = next(iter(settings))
        if "settings" in settings[key]:
            settings = settings[key]["settings"]
            if "index" in settings:
                settings = settings["index"]

    # Remove known-bad keys
    for bad_key in REMOVED_SETTINGS:
        # keys may be stored dotted or nested – try both
        parts = bad_key.split(".")
        # dotted
        if bad_key in settings:
            del settings[bad_key]
            logger.debug("Removed setting '%s'", bad_key)
        # nested (index.mapper.dynamic → settings["mapper"]["dynamic"])
        # we only strip top-level dotted for now

    # Always remove auto-managed settings - these are read-only
    for auto_key in AUTO_MANAGED_SETTINGS:
        # Try with and without 'index.' prefix
        for key_variant in [auto_key, f"index.{auto_key}"]:
            if key_variant in settings:
                del settings[key_variant]
                logger.debug("Removed auto-managed setting '%s'", key_variant)

    return settings


def transform_index(index_name: str,
                    raw_mapping: Dict[str, Any],
                    raw_settings: Dict[str, Any]) -> Tuple[Dict, Dict]:
    """
    High-level entry point.  Returns (mapping_8x, settings_8x) ready to
    pass directly to the 8.x create-index API.
    """
    logger.info("Transforming index '%s' …", index_name)

    mapping  = _strip_type_wrapper(raw_mapping)
    mapping  = _rename_deprecated_types(mapping)
    settings = clean_settings(raw_settings)

    logger.debug("Transformed mapping:\n%s", json.dumps(mapping, indent=2))
    return mapping, settings