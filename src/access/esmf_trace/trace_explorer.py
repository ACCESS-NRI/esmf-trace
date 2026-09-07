import colorsys
import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from string import Template

import pandas as pd
import plotly.graph_objects as go
from plotly.offline import get_plotlyjs

# The embedded CSS/JavaScript is intentionally compact.
# ruff: noqa: E501

GROUP_ORDER = (
    "Initialisation",
    "Run phase",
    "Finalisation",
    "Mediator",
    "Coupling",
    "Framework",
)

GROUP_COLOURS = {
    "Initialisation": "#B7791F",
    "Run phase": "#26734D",
    "Finalisation": "#C2413B",
    "Mediator": "#7C3AED",
    "Coupling": "#C026D3",
    "Framework": "#64748B",
}

GROUP_HELP = {
    "Initialisation": (
        "Set-up phases for the models and the NUOPC driver. "
        "IPDvXXpY means Initialize Phase Definition version XX, phase Y."
    ),
    "Run phase": "Time-stepping / model-advance phases, including the top-level coupled driver.",
    "Finalisation": "Shutdown, cleanup and finalize phases for models and the top-level driver.",
    "Mediator": "Mediator-specific preparation, post-processing, diagnostics and I/O phases.",
    "Coupling": "Directional data exchange between coupled components.",
    "Framework": "ESMF / ensemble runtime overhead outside named model lifecycle phases.",
}

_MODEL_COMPONENTS = ("ATM", "OCN", "ICE", "MED", "ROF")
_SEGMENT_RE = re.compile(r"^\[([^\]]+)\]\s*(.*)$")
_IPD_RE = re.compile(r"^IPDv(\d+)p(\d+)$")

# Concise NUOPC phase descriptions. Unknown IPD phases still receive a useful
# generic description from _ipd_fallback().
_MODEL_IPD_DESCRIPTIONS = {
    "IPDv01p1": "Advertise import/export fields.",
    "IPDv01p3": "Realize the connected fields.",
    "IPDv01p4": "Verify field connections and component clock.",
    "IPDv01p5": "Initialize export data and timestamps.",
    "IPDv03p1": "Advertise fields and geometry-transfer capability.",
    "IPDv03p3": "Realize fields that provide geometry.",
    "IPDv03p4": "Optionally adjust accepted-grid decomposition.",
    "IPDv03p5": "Realize fields that accept transferred geometry.",
    "IPDv03p6": "Verify connected fields and component clock.",
    "IPDv03p7": "Initialize export data and timestamps.",
}

_DRIVER_IPD_DESCRIPTIONS = {
    "IPDv02p1": "Set up the driver, child components, connectors and early initialization.",
    "IPDv02p3": "Continue driving child-component initialization through later phases.",
    "IPDv02p5": "Complete child initialization and initialization-data dependencies.",
    "IPDv03p2": "Coordinate connector coupling-list / connection metadata.",
    "IPDv05p1": "Mirror child fields and geometry onto the driver's states.",
    "IPDv05p2": "Reset the driver's field-mirroring request.",
    "IPDv05p3": "Configure driver connector coupling lists and redistribution.",
    "IPDv05p4": "Check that connected driver fields have producer connections.",
    "IPDv05p6": "Complete allocation of fields on the driver's states.",
    "IPDv05p8": "Complete initialization-data dependency bookkeeping.",
}

_MEDIATOR_DESCRIPTIONS = {
    "aofluxes_run": "Compute/apply atmosphere-ocean flux mediation.",
    "diag_accum": "Accumulate mediator diagnostics.",
    "diag_atm": "Atmosphere-related mediator diagnostics.",
    "diag_ice_ice2med": "Diagnostics for ICE -> MED exchange.",
    "diag_ice_med2ice": "Diagnostics for MED -> ICE exchange.",
    "diag_ocn": "Ocean-related mediator diagnostics.",
    "diag_print": "Print mediator diagnostic information.",
    "diag_rof": "Runoff-related mediator diagnostics.",
    "history_write": "Write mediator history output.",
    "ocnalb_run": "Mediator ocean-albedo work.",
    "post_atm": "Post-process atmosphere exchange.",
    "post_ice": "Post-process sea-ice exchange.",
    "post_ocn": "Post-process ocean exchange.",
    "post_rof": "Post-process runoff exchange.",
    "prep_ice": "Prepare fields for sea-ice exchange.",
    "prep_ocn_accum": "Accumulate fields for ocean preparation.",
    "prep_ocn_avg": "Average fields for ocean preparation.",
    "profile": "Mediator profiling / timing work.",
    "restart_write": "Write mediator restart data.",
    "scalefreshwater_run": "Scale/adjust freshwater fluxes.",
}

_COMPONENT_HUES = {
    "ATM": 30.0,
    "OCN": 164.0,
    "ICE": 205.0,
    "MED": 288.0,
    "ROF": 8.0,
    "Driver": 224.0,
    "ESMF": 216.0,
}


@dataclass(frozen=True)
class TraceSelector:
    group: str
    component: str
    label: str

    @property
    def leaf(self) -> str:
        if self.group == "Mediator":
            return f"Mediator::{self.label}"
        if self.group == "Coupling":
            return f"Coupling::{self.component}"
        if self.group == "Framework":
            return "Framework::ESMF::runtime-overhead"
        return f"{self.group}::{self.component}::{self.label}"


def _segments(path: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for part in str(path).split("/"):
        match = _SEGMENT_RE.match(part.strip())
        if match:
            out.append((match.group(1), match.group(2).strip()))
    return out


def classify_trace_path(path: str) -> TraceSelector:
    """Map a hierarchical ESMF region path to the phase-first explorer taxonomy."""
    segments = _segments(path)

    # Work from the deepest named ESMF component towards the root. Internal
    # routines below a component phase inherit the nearest component phase.
    for tag, label in reversed(segments):
        upper = tag.upper()

        if "-TO-" in upper:
            direction = upper
            return TraceSelector("Coupling", direction, direction.replace("-TO-", " -> "))

        if upper in _MODEL_COMPONENTS:
            if label.startswith("Init") or _IPD_RE.match(label):
                return TraceSelector("Initialisation", upper, label or "Init")
            if label.startswith("RunPhase"):
                return TraceSelector("Run phase", upper, label)
            if label.startswith("Finalize"):
                return TraceSelector("Finalisation", upper, label)
            if upper == "MED":
                med_label = label.removeprefix("med_phases_") if label else "Mediator"
                return TraceSelector("Mediator", "MED", med_label)

        # ESM0001 is the NUOPC top-level driver, not generic framework overhead.
        if upper.startswith("ESM") and upper != "ESMF":
            if label.startswith("RunPhase"):
                return TraceSelector("Run phase", "Driver", label)
            if label.startswith("Finalize"):
                return TraceSelector("Finalisation", "Driver", label)
            if label.startswith("Init") or _IPD_RE.match(label):
                return TraceSelector("Initialisation", "Driver", label or "Init")

    return TraceSelector("Framework", "ESMF", "ESMF / ensemble overhead")


def _ipd_fallback(label: str) -> str:
    match = _IPD_RE.match(label)
    if not match:
        return "NUOPC component initialization phase."
    version, phase = match.groups()
    return f"NUOPC Initialize Phase Definition v{version}, phase {phase}."


def selector_description(selector: TraceSelector) -> str:
    group, component, label = selector.group, selector.component, selector.label

    if group == "Initialisation":
        if label.startswith("Init"):
            if component == "Driver":
                return "Set the driver's initialize-phase definitions and core setup."
            return "Initial component setup / initialize-phase mapping."
        if component == "Driver":
            return _DRIVER_IPD_DESCRIPTIONS.get(label, _ipd_fallback(label))
        return _MODEL_IPD_DESCRIPTIONS.get(label, _ipd_fallback(label))

    if group == "Run phase":
        if component == "Driver":
            return "Drive the configured coupled-model run sequence for a time step."
        return f"Advance the {component} component during the coupled run."

    if group == "Finalisation":
        if component == "Driver":
            return "Finalize child components/connectors and clean up driver resources."
        return f"Finalize and clean up the {component} component."

    if group == "Mediator":
        return _MEDIATOR_DESCRIPTIONS.get(label, "Mediator processing phase.")

    if group == "Coupling":
        return f"Transfer/couple data for {label}."

    return "ESMF/ensemble framework overhead outside the named model lifecycle phases."


def annotate_selector_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add explorer taxonomy columns without modifying the caller's DataFrame."""
    out = df.copy()
    selectors = out["model_component"].map(classify_trace_path)
    out["phase_group"] = [item.group for item in selectors]
    out["phase_component"] = [item.component for item in selectors]
    out["phase_label"] = [item.label for item in selectors]
    out["phase_leaf"] = [item.leaf for item in selectors]
    return out


def _hls_hex(hue_deg: float, lightness: float, saturation: float) -> str:
    rgb = colorsys.hls_to_rgb((hue_deg % 360.0) / 360.0, lightness, saturation)
    return "#" + "".join(f"{round(value * 255):02X}" for value in rgb)


def _stable_offset(key: str, width: float) -> float:
    digest = hashlib.sha1(key.encode()).digest()
    unit = int.from_bytes(digest[:2], "big") / 65535.0
    return (unit - 0.5) * width


def selector_colour_map(selectors: dict[str, TraceSelector]) -> dict[str, str]:
    """Return stable, distinct colours while keeping related components visually related."""
    by_component: dict[str, list[str]] = defaultdict(list)
    for leaf, selector in selectors.items():
        by_component[selector.component].append(leaf)

    colours: dict[str, str] = {}
    for component, leaves in by_component.items():
        leaves.sort()

        if component == "ESMF":
            for index, leaf in enumerate(leaves):
                lightness = 0.40 + 0.12 * (index % 3)
                colours[leaf] = _hls_hex(216.0 + _stable_offset(leaf, 22.0), lightness, 0.20)
            continue

        if "-TO-" in component:
            for leaf in leaves:
                hue = (320.0 + _stable_offset(component, 300.0)) % 360.0
                colours[leaf] = _hls_hex(hue, 0.47, 0.72)
            continue

        base_hue = _COMPONENT_HUES.get(component, 250.0 + _stable_offset(component, 180.0))
        light_cycle = (0.39, 0.50, 0.61, 0.45, 0.56)
        for index, leaf in enumerate(leaves):
            # Sorted leaves get a small, deterministic hue spread around the
            # component hue; the lightness cycle keeps many MED/IPD entries distinct.
            spread = 0.0 if len(leaves) == 1 else ((index / (len(leaves) - 1)) - 0.5) * 34.0
            colours[leaf] = _hls_hex(base_hue + spread, light_cycle[index % len(light_cycle)], 0.70)

    # Collision guard: unlikely, but exact uniqueness is a useful invariant for the explorer.
    used: dict[str, str] = {}
    for leaf in sorted(colours):
        colour = colours[leaf]
        if colour in used:
            colours[leaf] = _hls_hex(int(hashlib.sha1(leaf.encode()).hexdigest()[:4], 16) / 65535 * 360, 0.48, 0.72)
        used[colours[leaf]] = leaf
    return colours


def _section_for(selector: TraceSelector) -> str:
    if selector.group == "Mediator":
        return "Mediator phases"
    if selector.group == "Coupling":
        return "Directions"
    if selector.group == "Framework":
        return "ESMF framework"
    return selector.component


def _leaf_sort_key(leaf: str, selector: TraceSelector) -> tuple:
    label = selector.label
    if label.startswith("Init"):
        return (0, label)
    match = _IPD_RE.match(label)
    if match:
        return (1, int(match.group(1)), int(match.group(2)))
    if label.startswith("RunPhase"):
        return (2, label)
    if label.startswith("Finalize"):
        return (3, label)
    return (4, label)


def build_menu(selectors: dict[str, TraceSelector]) -> dict[str, list[dict]]:
    section_priority = {
        "Initialisation": ["ATM", "OCN", "ICE", "MED", "ROF", "Driver"],
        "Run phase": ["ATM", "OCN", "ICE", "MED", "ROF", "Driver"],
        "Finalisation": ["ATM", "OCN", "ICE", "MED", "ROF", "Driver"],
        "Mediator": ["Mediator phases"],
        "Coupling": ["Directions"],
        "Framework": ["ESMF framework"],
    }

    menu: dict[str, list[dict]] = {}
    for group in GROUP_ORDER:
        sections: dict[str, list[str]] = defaultdict(list)
        for leaf, selector in selectors.items():
            if selector.group == group:
                sections[_section_for(selector)].append(leaf)

        ordered_names = section_priority[group] + [name for name in sections if name not in section_priority[group]]
        menu[group] = []
        for section in ordered_names:
            if section not in sections:
                continue
            leaves = sorted(sections[section], key=lambda leaf: _leaf_sort_key(leaf, selectors[leaf]))
            menu[group].append({"section": section, "leafs": leaves})
    return menu


def build_explorer_payload(fig: go.Figure, df: pd.DataFrame, xaxis_datetime: bool) -> dict:
    """Build the small UI index around the Plotly data without duplicating span-level strings."""
    selectors = {
        leaf: TraceSelector(group, component, label)
        for leaf, group, component, label in df[["phase_leaf", "phase_group", "phase_component", "phase_label"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    }
    colours = selector_colour_map(selectors)

    # Apply colours/metadata after all selectors are known. One trace remains
    # one logical timing region, preserving the original region count.
    for trace in fig.data:
        meta = dict(trace.meta) if isinstance(trace.meta, dict) else {}
        leaf = meta["leaf"]
        trace.marker.color = colours[leaf]
        trace.marker.line = {"color": "rgba(17,24,39,0.52)", "width": 0.55}

    figure_json = json.loads(fig.to_json())

    y_info = df[["depth", "pet", "y_cat"]].drop_duplicates().sort_values(["depth", "pet"])
    y_categories = y_info["y_cat"].tolist()
    y_labels = {row.y_cat: f"Depth {int(row.depth)} · PET {int(row.pet)}" for row in y_info.itertuples(index=False)}
    depth_values = sorted(int(value) for value in df["depth"].unique())
    depth_categories = {str(depth): y_info.loc[y_info["depth"] == depth, "y_cat"].tolist() for depth in depth_values}

    if xaxis_datetime:
        full_x = [df["x_start"].min().isoformat(), df["x_end"].max().isoformat()]
    else:
        full_x = [0.0, float(df["x_end"].max())]

    leaf_info = {
        leaf: {
            "group": selector.group,
            "component": selector.component,
            "leaf": leaf,
            "label": selector.label,
            "section": _section_for(selector),
            "description": selector_description(selector),
        }
        for leaf, selector in selectors.items()
    }

    return {
        "data": figure_json["data"],
        "leafInfo": leaf_info,
        "leafColors": colours,
        "menu": build_menu(selectors),
        "groups": [group for group in GROUP_ORDER if any(item.group == group for item in selectors.values())],
        "groupColors": GROUP_COLOURS,
        "groupHelp": GROUP_HELP,
        "yCategories": y_categories,
        "yLabels": y_labels,
        "depthValues": depth_values,
        "depthCategories": depth_categories,
        "fullX": full_x,
        "xaxisDatetime": xaxis_datetime,
        "regionCount": len(fig.data),
        "spanCount": int(len(df)),
        "petCount": int(df["pet"].nunique()),
    }


TRACE_EXPLORER_CSS = r"""
:root{--header:178px;--line:#e4e8ef;--text:#172033;--muted:#667085}
*{box-sizing:border-box}html,body{margin:0;width:100%;height:100%;overflow:hidden;background:#fff;color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
button,input{font:inherit}
#boot{position:fixed;inset:0;z-index:99;background:#f8fafc;display:flex;align-items:center;justify-content:center;transition:opacity .18s}.loader{width:min(430px,80vw);background:#fff;border:1px solid var(--line);border-radius:16px;padding:24px;box-shadow:0 18px 45px rgba(16,24,40,.09)}.loader b{font-size:16px}.loader p{font-size:11px;color:var(--muted);margin:6px 0 14px}.bar{height:5px;background:#edf0f5;border-radius:99px;overflow:hidden}.bar i{display:block;height:100%;width:34%;background:linear-gradient(90deg,#4f46e5,#a855f7);animation:go 1.1s ease-in-out infinite alternate}@keyframes go{from{transform:translateX(-90%)}to{transform:translateX(270%)}}
#header{height:var(--header);border-bottom:1px solid var(--line);position:relative;z-index:20;background:#fff}#top{height:62px;display:flex;align-items:center;gap:15px;padding:9px 18px;border-bottom:1px solid #f0f2f5}.brand{min-width:220px}.brand strong{font-size:16px;display:flex;align-items:center;gap:9px}.brand strong:before{content:"";width:10px;height:10px;border-radius:50%;background:linear-gradient(135deg,#4f46e5,#a855f7);box-shadow:0 0 0 4px #eef0ff}.brand small{display:block;margin-top:3px;font-size:10.5px;color:var(--muted)}
.stats{display:flex;gap:6px}.stat{font-size:10px;color:var(--muted);padding:6px 8px;border:1px solid var(--line);border-radius:8px;background:#fafbfc}.stat b{color:var(--text)}.tools{margin-left:auto;display:flex;align-items:center;gap:7px}.search{width:min(315px,25vw)}.search input{width:100%;height:34px;border:1px solid var(--line);border-radius:9px;padding:0 10px;font-size:10.5px;outline:none}.tool{height:34px;border:1px solid var(--line);border-radius:9px;background:#fff;color:#475467;padding:0 10px;font-size:10.5px;font-weight:630;cursor:pointer}.tool:hover{background:#f8f9ff;border-color:#cbd2ff}.tool.primary{color:#37419b;border-color:#cbd2ff;background:#f8f9ff}#status{font-size:9.5px;color:var(--muted);min-width:135px;text-align:right}
#filters{height:116px;padding:8px 18px 10px;background:linear-gradient(#fff,#fbfcff);display:flex;flex-direction:column;justify-content:center;gap:9px}.filter{display:grid;grid-template-columns:215px minmax(0,1fr);align-items:center;gap:10px}.filter h4{font-size:11px;margin:0;color:#344054}.filter p{font-size:9.3px;color:#7d8798;margin:2px 0 0;line-height:1.3}.chips{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.chip{height:29px;border:1px solid #dfe4ec;border-radius:8px;background:#fff;color:#475467;padding:0 9px;font-size:10px;cursor:pointer;display:inline-flex;align-items:center;gap:6px}.chip .label:before{content:"";display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--c,#667085);margin-right:6px}.chip.depth .label:before{display:none}.chip.active{font-weight:720;color:#101828;border-color:var(--c,#4f46e5);background:color-mix(in srgb,var(--c,#4f46e5) 11%,white)}.chip.partial{font-weight:700;border-color:var(--c,#4f46e5);background:repeating-linear-gradient(135deg,#fff,#fff 5px,color-mix(in srgb,var(--c,#4f46e5) 9%,white) 5px,color-mix(in srgb,var(--c,#4f46e5) 9%,white) 10px)}.chip .count{font-size:8px;border-left:1px solid #e4e7ec;padding-left:5px;color:#667085}.chev{font-size:9px;color:#7d8798}
#plot{position:absolute;top:var(--header);bottom:0;left:0;right:0}#empty{position:absolute;z-index:8;left:50%;top:55%;transform:translate(-50%,-50%);display:none;background:#fff;border:1px dashed #cdd4df;border-radius:10px;padding:12px 15px;color:#667085;font-size:11px}
#popover{position:fixed;z-index:45;display:none;flex-direction:column;max-height:min(520px,calc(100vh - 80px));background:#fff;border:1px solid #dfe4ec;border-radius:14px;box-shadow:0 20px 50px rgba(16,24,40,.16);overflow:hidden}#popover.open{display:flex}.phead{display:flex;align-items:center;gap:10px;padding:12px 14px;border-bottom:1px solid #edf0f4}.ptitle{min-width:155px}.ptitle b{display:block;font-size:11.5px}.ptitle small{display:block;font-size:9px;color:#7d8798;margin-top:2px;max-width:300px}.phead input{flex:1;height:32px;border:1px solid var(--line);border-radius:8px;padding:0 9px;font-size:10px;outline:none}#pop-body{overflow:auto;padding:6px 14px 14px}.section{padding:9px 0}.section+.section{border-top:1px solid #edf0f4}.section-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:7px}.section-name{font-size:10.5px;font-weight:760}.section-all{border:0;background:transparent;color:#4f46e5;font-size:9px;cursor:pointer}.leaf-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:5px}.leaf{height:48px;min-width:0;border:1px solid #e1e5ec;border-radius:7px;background:#fff;color:#475467;font-size:9.2px;display:flex;align-items:flex-start;gap:7px;padding:6px 8px;cursor:pointer;text-align:left}.leaf:before{content:"";width:7px;height:7px;flex:0 0 auto;border-radius:50%;background:var(--c,#667085);margin-top:4px}.leaf-copy{display:flex;flex-direction:column;align-items:flex-start;gap:2px;min-width:0;overflow:hidden}.leaf-name{display:block;width:100%;font-size:10.5px;font-weight:650;color:#344054;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.leaf-desc{display:block;width:100%;font-size:9px;font-weight:450;line-height:1.2;color:#7a8495;white-space:normal;overflow:hidden}.leaf.active{border-color:var(--c,#4f46e5);background:color-mix(in srgb,var(--c,#4f46e5) 10%,white)}.leaf.active .leaf-name{color:#101828}
#inspector{position:absolute;left:16px;bottom:14px;z-index:12;max-width:min(820px,72vw);background:rgba(17,24,39,.93);color:#fff;border-radius:10px;padding:8px 11px;opacity:0;pointer-events:none;transition:opacity .1s}#inspector.show{opacity:1}#inspect-path{font-size:10.5px;font-weight:620;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}#inspect-meta{font-size:9.5px;color:#d0d5dd;margin-top:3px}
@media(max-width:1120px){.stats{display:none}.filter{grid-template-columns:180px minmax(0,1fr)}.leaf-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
"""

TRACE_EXPLORER_JS = r"""
const P=$payload;
const GROUP_LABEL={
  "Initialisation":"Initialisation",
  "Run phase":"Run phase",
  "Finalisation":"Finalisation",
  "Mediator":"Mediator",
  "Coupling":"Coupling",
  "Framework":"Framework"
};

const activeLeaves=new Set();
const activeDepths=new Set();
let searchQuery="";
let currentPopoverGroup=null;
let openTimer=null,closeTimer=null;
// Keep the original X domain separate from Plotly's mutable layout objects.
// Plotly may mutate layout.xaxis.range during zoom/pan, so never pass P.fullX
// itself into the layout or use it as the reset source afterwards.
const FULL_X=Object.freeze(P.fullX.slice());
let plotOperationRunning=false,filterDirty=false,resetPending=false,updateScheduled=false;
const allTraceIndices=P.data.map((_,i)=>i);

document.getElementById("stat-spans").textContent=P.spanCount.toLocaleString();
document.getElementById("stat-regions").textContent=P.regionCount.toLocaleString();
document.getElementById("stat-pets").textContent=P.petCount.toLocaleString();

function groupLeaves(group){
  const out=[];
  for(const sec of P.menu[group]||[])for(const leaf of sec.leafs)out.push(leaf);
  return out;
}

function makeChip(label,key,color,depth=false){
  const b=document.createElement("button");
  b.className="chip"+(depth?" depth":"");
  b.dataset.key=String(key);b.style.setProperty("--c",color);
  b.innerHTML=depth?`<span class="label">${label}</span>`:`<span class="label">${label}</span><span class="chev">▾</span>`;
  return b;
}

const groupWrap=document.getElementById("group-chips");
const allGroup=makeChip("ALL","ALL","#4F46E5",true);
allGroup.onclick=()=>{activeLeaves.clear();refreshControls();scheduleUpdate()};
groupWrap.appendChild(allGroup);
const groupButtons=new Map();
for(const group of P.groups){
  const b=makeChip(GROUP_LABEL[group],group,P.groupColors[group],false);
  b.onclick=(ev)=>{
    const members=groupLeaves(group);
    const full=activeLeaves.size>0&&members.every(k=>activeLeaves.has(k));
    if(activeLeaves.size===0)for(const k of members)activeLeaves.add(k);
    else if(full){for(const k of members)activeLeaves.delete(k);if(activeLeaves.size===0)activeLeaves.clear()}
    else for(const k of members)activeLeaves.add(k);
    refreshControls();scheduleUpdate();openPopover(group,b);ev.stopPropagation();
  };
  b.onmouseenter=()=>{clearTimeout(closeTimer);openTimer=setTimeout(()=>openPopover(group,b),150)};
  b.onmouseleave=()=>{clearTimeout(openTimer);closeTimer=setTimeout(closePopover,330)};
  groupButtons.set(group,b);groupWrap.appendChild(b);
}

const depthWrap=document.getElementById("depth-chips");
const allDepth=makeChip("ALL","ALL","#4F46E5",true);
allDepth.onclick=()=>{activeDepths.clear();refreshControls();scheduleUpdate()};
depthWrap.appendChild(allDepth);
const depthButtons=new Map();
for(const depth of P.depthValues){
  const b=makeChip(String(depth),depth,"#4F46E5",true);
  b.onclick=()=>{
    if(activeDepths.size===0)activeDepths.add(depth);
    else if(activeDepths.has(depth)){activeDepths.delete(depth);if(activeDepths.size===0)activeDepths.clear()}
    else activeDepths.add(depth);
    refreshControls();scheduleUpdate();
  };
  depthButtons.set(depth,b);depthWrap.appendChild(b);
}

function refreshControls(){
  allGroup.classList.toggle("active",activeLeaves.size===0);
  for(const [group,b] of groupButtons){
    const members=groupLeaves(group);
    const n=activeLeaves.size===0?0:members.filter(k=>activeLeaves.has(k)).length;
    const full=n>0&&n===members.length,partial=n>0&&n<members.length;
    b.classList.toggle("active",full);b.classList.toggle("partial",partial);
    let count=b.querySelector(".count");
    if(partial){if(!count){count=document.createElement("span");count.className="count";b.appendChild(count)}count.textContent=`${n}/${members.length}`}
    else if(count)count.remove();
  }
  allDepth.classList.toggle("active",activeDepths.size===0);
  for(const [depth,b] of depthButtons)b.classList.toggle("active",activeDepths.has(depth));
  document.querySelectorAll(".leaf").forEach(b=>b.classList.toggle("active",activeLeaves.size>0&&activeLeaves.has(b.dataset.leaf)));
}

const pop=document.getElementById("popover"),popBody=document.getElementById("pop-body"),popSearch=document.getElementById("pop-search");
pop.onmouseenter=()=>clearTimeout(closeTimer);pop.onmouseleave=()=>{closeTimer=setTimeout(closePopover,330)};pop.onclick=ev=>ev.stopPropagation();
function positionPopover(anchor){const r=anchor.getBoundingClientRect(),width=Math.min(780,window.innerWidth-32);const left=Math.max(16,Math.min(r.left,window.innerWidth-width-16));const top=Math.min(r.bottom+6,window.innerHeight-220);pop.style.left=`${left}px`;pop.style.top=`${top}px`;pop.style.width=`${width}px`}
function openPopover(group,anchor){clearTimeout(closeTimer);currentPopoverGroup=group;positionPopover(anchor);document.getElementById("pop-title").textContent=GROUP_LABEL[group];document.getElementById("pop-sub").textContent=P.groupHelp[group]||"Select the whole group or individual regions.";popSearch.value="";renderPopover(group,"");pop.classList.add("open")}
function closePopover(){pop.classList.remove("open");currentPopoverGroup=null}
document.addEventListener("click",closePopover);document.addEventListener("keydown",ev=>{if(ev.key==="Escape")closePopover()});popSearch.oninput=()=>{if(currentPopoverGroup)renderPopover(currentPopoverGroup,popSearch.value)};

function renderPopover(group,filterText){
  popBody.innerHTML="";const needle=filterText.trim().toLowerCase();
  for(const sec of P.menu[group]||[]){
    const leaves=sec.leafs.filter(leaf=>{const info=P.leafInfo[leaf];return !needle||info.label.toLowerCase().includes(needle)||info.component.toLowerCase().includes(needle)||(info.description||"").toLowerCase().includes(needle)||leaf.toLowerCase().includes(needle)});
    if(!leaves.length)continue;
    const section=document.createElement("div");section.className="section";section.innerHTML=`<div class="section-head"><span class="section-name">${sec.section}</span><button class="section-all">Select section</button></div><div class="leaf-grid"></div>`;
    const grid=section.querySelector(".leaf-grid");
    section.querySelector(".section-all").onclick=()=>{
      const full=activeLeaves.size>0&&leaves.every(k=>activeLeaves.has(k));
      if(activeLeaves.size===0)for(const k of leaves)activeLeaves.add(k);
      else if(full){for(const k of leaves)activeLeaves.delete(k);if(activeLeaves.size===0)activeLeaves.clear()}
      else for(const k of leaves)activeLeaves.add(k);
      refreshControls();scheduleUpdate();renderPopover(group,filterText);
    };
    for(const leaf of leaves){
      const info=P.leafInfo[leaf],b=document.createElement("button");b.className="leaf";b.dataset.leaf=leaf;b.style.setProperty("--c",P.leafColors[leaf]);b.title=`${info.component} · ${info.label}`;
      const displayLabel=group==="Mediator"?info.label:`${info.component} · ${info.label}`;
      b.innerHTML=`<span class="leaf-copy"><span class="leaf-name">${displayLabel}</span><span class="leaf-desc">${info.description||""}</span></span>`;
      b.onclick=()=>{if(activeLeaves.size===0)activeLeaves.add(leaf);else if(activeLeaves.has(leaf)){activeLeaves.delete(leaf);if(activeLeaves.size===0)activeLeaves.clear()}else activeLeaves.add(leaf);refreshControls();scheduleUpdate();renderPopover(group,filterText)};
      grid.appendChild(b);
    }
    popBody.appendChild(section);
  }
  refreshControls();
}

function selectedYCats(){
  if(activeDepths.size===0)return P.yCategories.slice();
  const out=[];for(const cat of P.yCategories){const match=cat.match(/depth(\d+)/);if(match&&activeDepths.has(Number(match[1])))out.push(cat)}return out;
}
function traceVisible(trace){
  const m=trace.meta,leafOK=activeLeaves.size===0||activeLeaves.has(m.leaf),depthOK=activeDepths.size===0||activeDepths.has(m.depth),q=searchQuery.trim().toLowerCase();
  const searchOK=!q||m.path.toLowerCase().includes(q)||m.label.toLowerCase().includes(q)||m.component.toLowerCase().includes(q);
  return leafOK&&depthOK&&searchOK;
}
function scheduleUpdate(){
  filterDirty=true;
  if(updateScheduled)return;
  updateScheduled=true;
  requestAnimationFrame(()=>{updateScheduled=false;runPlotOperations()});
}
function scheduleResetView(){resetPending=true;runPlotOperations()}
async function runPlotOperations(){
  if(plotOperationRunning)return;
  plotOperationRunning=true;
  try{
    while(filterDirty||resetPending){
      if(filterDirty){filterDirty=false;await applyFilterUpdate()}
      if(resetPending){resetPending=false;await applyResetView()}
    }
  }finally{
    plotOperationRunning=false;
    if(filterDirty||resetPending)runPlotOperations();
  }
}
async function applyFilterUpdate(){
  const graph=document.getElementById("plot");if(!graph._fullLayout)return;const start=performance.now();
  const visibility=P.data.map(traceVisible),ycats=selectedYCats(),yRange=ycats.length?[ycats.length-.5,-.5]:[.5,-.5];
  const layoutUpdate={"yaxis.categoryarray":ycats,"yaxis.tickvals":ycats,"yaxis.ticktext":ycats.map(c=>P.yLabels[c]),"yaxis.range":yRange,"yaxis.autorange":false};
  // Filtering changes visibility/Y rows only and never touches the X range.
  await Plotly.update(graph,{visible:visibility},layoutUpdate,allTraceIndices);
  const visibleRegions=visibility.reduce((a,v)=>a+(v?1:0),0);document.getElementById("status").textContent=`${visibleRegions}/${P.regionCount} regions · ${Math.round(performance.now()-start)} ms`;document.getElementById("empty").style.display=visibleRegions?"none":"block";
}
async function applyResetView(){
  const graph=document.getElementById("plot");if(!graph._fullLayout)return;const ycats=selectedYCats(),yRange=ycats.length?[ycats.length-.5,-.5]:[.5,-.5];
  // Reset has one invariant meaning: immutable original X + current depth-selected Y.
  await Plotly.relayout(graph,{"xaxis.range[0]":FULL_X[0],"xaxis.range[1]":FULL_X[1],"xaxis.autorange":false,"yaxis.categoryarray":ycats,"yaxis.tickvals":ycats,"yaxis.ticktext":ycats.map(c=>P.yLabels[c]),"yaxis.range":yRange,"yaxis.autorange":false});
}

document.getElementById("global-search").oninput=ev=>{searchQuery=ev.target.value;scheduleUpdate()};
document.getElementById("clear").onclick=()=>{activeLeaves.clear();activeDepths.clear();searchQuery="";document.getElementById("global-search").value="";refreshControls();scheduleUpdate()};
document.getElementById("reset").onclick=scheduleResetView;

const layout={
  bargap:0,barmode:"overlay",showlegend:false,paper_bgcolor:"#fff",plot_bgcolor:"#fff",margin:{l:145,r:24,t:16,b:64},dragmode:"zoom",uirevision:"esmf-trace-explorer",
  xaxis:{title:{text:P.xaxisDatetime?"Wall-clock time":"Elapsed time (s)",font:{size:14,color:"#1F2937"},standoff:12},range:FULL_X.slice(),autorange:false,fixedrange:false,tickfont:{size:13,color:"#344054"},showgrid:true,gridcolor:"rgba(100,116,139,.18)",zeroline:false},
  yaxis:{title:{text:"Stack depth / PET",font:{size:14,color:"#1F2937"},standoff:12},type:"category",categoryorder:"array",categoryarray:P.yCategories,range:[P.yCategories.length-.5,-.5],autorange:false,fixedrange:true,tickmode:"array",tickvals:P.yCategories,ticktext:P.yCategories.map(c=>P.yLabels[c]),tickfont:{size:13,color:"#344054"},showgrid:true,gridcolor:"rgba(17,24,39,.24)",griddash:"dot"},
  hovermode:"closest",hoverlabel:{bgcolor:"#111827",font:{size:11,color:"#fff"},bordercolor:"#111827"}
};
const config={responsive:true,displaylogo:false,scrollZoom:true,doubleClick:false,modeBarButtonsToRemove:["autoScale2d","resetScale2d","select2d","lasso2d"],toImageButtonOptions:{filename:"esmf_trace"}};
Plotly.newPlot("plot",P.data,layout,config).then(graph=>{
  refreshControls();document.getElementById("status").textContent=`${P.regionCount}/${P.regionCount} regions`;document.getElementById("boot").style.opacity="0";setTimeout(()=>document.getElementById("boot").remove(),180);
  const inspector=document.getElementById("inspector");graph.on("plotly_hover",ev=>{const pt=ev.points&&ev.points[0];if(!pt)return;const m=pt.data.meta;document.getElementById("inspect-path").textContent=m.path;const start=P.xaxisDatetime?String(pt.base):`${Number(pt.base).toFixed(6)} s`;const duration=`${Number(pt.customdata).toFixed(6)} s`;document.getElementById("inspect-meta").textContent=`${m.group} · ${m.component} · ${m.label} · PET ${m.pet} · depth ${m.depth} · start ${start} · duration ${duration}`;inspector.classList.add("show")});graph.on("plotly_unhover",()=>inspector.classList.remove("show"));
});
window.addEventListener("resize",()=>{if(currentPopoverGroup){const b=groupButtons.get(currentPopoverGroup);if(b)positionPopover(b)}});
"""

TRACE_EXPLORER_TEMPLATE = Template(r"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ESMF Trace Explorer</title>
<style>$css</style>
</head>
<body>
<div id="boot"><div class="loader"><b>ESMF Trace Explorer</b><p>Preparing the interactive timing view...</p><div class="bar"><i></i></div></div></div>
<div id="header">
  <div id="top">
    <div class="brand"><strong>ESMF Trace Explorer</strong><small>Interactive performance timeline</small></div>
    <div class="stats"><span class="stat"><b id="stat-spans"></b> spans</span><span class="stat"><b id="stat-regions"></b> regions</span><span class="stat"><b id="stat-pets"></b> PETs</span></div>
    <div class="tools"><div class="search"><input id="global-search" placeholder="Search full timing path..."></div><span id="status"></span><button class="tool" id="clear">Clear filters</button><button class="tool primary" id="reset">Reset view</button></div>
  </div>
  <div id="filters">
    <div class="filter"><div><h4>Phase / region group</h4><p>Choose a lifecycle group; hover or click ▾ for component phases and short explanations.</p></div><div id="group-chips" class="chips"></div></div>
    <div class="filter"><div><h4>Stack depth</h4><p>Depth alone controls the Y rows. Component filters never collapse the depth axis.</p></div><div id="depth-chips" class="chips"></div></div>
  </div>
</div>
<div id="plot"></div><div id="empty">No timing regions match the current phase/component/depth selection.</div>
<div id="popover"><div class="phead"><div class="ptitle"><b id="pop-title"></b><small id="pop-sub"></small></div><input id="pop-search" placeholder="Search this group..."></div><div id="pop-body"></div></div>
<div id="inspector"><div id="inspect-path"></div><div id="inspect-meta"></div></div>
<script>$plotly_js</script>
<script>$javascript</script>
</body>
</html>""")


def write_trace_explorer_html(
    fig: go.Figure,
    df: pd.DataFrame,
    html_path: Path,
    xaxis_datetime: bool = False,
) -> None:
    """Write the self-contained, phase-first ESMF Trace Explorer HTML."""
    payload = build_explorer_payload(fig, df, xaxis_datetime=xaxis_datetime)
    payload_json = json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")
    javascript = Template(TRACE_EXPLORER_JS).safe_substitute(payload=payload_json)
    html = TRACE_EXPLORER_TEMPLATE.substitute(
        css=TRACE_EXPLORER_CSS,
        plotly_js=get_plotlyjs(),
        javascript=javascript,
    )
    html_path = Path(html_path)
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(html)
