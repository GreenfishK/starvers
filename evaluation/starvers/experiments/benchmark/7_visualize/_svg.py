"""Minimal SVG -> matplotlib Path loader.

Only supports the subset of SVG that the ingest-plot policy icons use:
  * absolute path commands  M, L, C, Z
  * multiple <path> elements are unioned into a single matplotlib Path

The icons are authored to this subset on purpose so no external SVG/JS
library is required at build time.
"""
import re
from pathlib import Path

import numpy as np
from matplotlib.path import Path as MPath

_NUM = r"[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?"
_CMD_RE = re.compile(_NUM + r"|[MmLlCcZz]")

# matplotlib's default path fill uses the even-odd rule, so matching SVG
# 'evenodd' is automatic. We keep this constant for clarity.
_FILLRULE_EVENODD = "evenodd"


def _parse_path(d: str):
    """Tokenize an absolute M/L/C/Z path into a flat (vert, code) list."""
    tokens = re.findall(_NUM + r"|[MmLlCcZz]", d)
    verts = []
    codes = []
    cur = np.array([0.0, 0.0])
    sub_start = np.array([0.0, 0.0])
    i = 0
    n = len(tokens)
    while i < n:
        tk = tokens[i]
        if tk in ("Z", "z"):
            verts.append(sub_start.copy())
            codes.append(MPath.CLOSEPOLY)
            cur = sub_start.copy()
            i += 1
            continue
        if not tk.isalpha():
            i += 1
            continue
        cmd = tk.upper()
        rel = tk.islower()
        i += 1
        # gather the numeric params that follow
        params = []
        while i < n and not tokens[i].isalpha():
            params.append(float(tokens[i]))
            i += 1
        if cmd == "M":
            for j in range(0, len(params), 2):
                p = np.array(params[j:j + 2])
                if rel:
                    p = cur + p
                if j == 0:
                    verts.append(p)
                    codes.append(MPath.MOVETO)
                    sub_start = p.copy()
                else:
                    verts.append(p)
                    codes.append(MPath.LINETO)
                cur = p
        elif cmd == "L":
            for j in range(0, len(params), 2):
                p = np.array(params[j:j + 2])
                if rel:
                    p = cur + p
                verts.append(p)
                codes.append(MPath.LINETO)
                cur = p
        elif cmd == "C":
            for j in range(0, len(params), 6):
                c1 = np.array(params[j:j + 2])
                c2 = np.array(params[j + 2:j + 4])
                p = np.array(params[j + 4:j + 6])
                if rel:
                    c1, c2, p = c1 + cur, c2 + cur, p + cur
                verts.extend([c1, c2, p])
                codes.extend([MPath.CURVE4] * 3)
                cur = p
    return np.array(verts), np.array(codes)


def load_icon_svg(svg_path) -> MPath:
    """Load a single-colour policy icon into a matplotlib Path.

    The path is normalized to the SVG viewBox, y-flipped to matplotlib's
    convention, and re-centred on the origin so it can be used directly as a
    scatter marker.
    """
    text = Path(svg_path).read_text(encoding="utf-8")
    vb = re.search(r'viewBox="([^"]*)"', text)
    if vb:
        x0, y0, w, h = [float(v) for v in re.findall(_NUM, vb.group(1))[:4]]
    else:
        x0, y0, w, h = 0.0, 0.0, 24.0, 24.0

    # Path data: outermost quoted d= attribute per <path> element.
    path_ds = re.findall(r'<path\b[^>]*\bd="([^"]*)"', text)
    if not path_ds:
        raise ValueError(f"No <path> elements found in SVG: {svg_path}")

    all_verts = []
    all_codes = []
    for d in path_ds:
        v, c = _parse_path(d)
        all_verts.append(v)
        all_codes.append(c)
    verts = np.vstack(all_verts)
    codes = np.concatenate(all_codes)
    # translate into viewBox-local coordinates then centre on origin
    verts = verts - np.array([x0, y0])
    verts = np.column_stack([verts[:, 0], h - verts[:, 1]])  # flip y
    # Centre the content's bounding box on the origin and normalise its size so
    # all icons are drawn at a consistent scale regardless of their viewBox.
    cx = (verts[:, 0].min() + verts[:, 0].max()) / 2.0
    cy = (verts[:, 1].min() + verts[:, 1].max()) / 2.0
    verts = verts - np.array([cx, cy])
    half = max(verts[:, 0].max() - verts[:, 0].min(),
               verts[:, 1].max() - verts[:, 1].min())
    target_half = 1.0
    if half > 0:
        verts = verts * (target_half / half)
    return MPath(verts, codes)
