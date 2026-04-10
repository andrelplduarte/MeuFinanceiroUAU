"""
Remove Photoshop/Chequerboard-style grey background from static/img/hero-central-uau.png
using constrained flood fill (edges + center) — only Pillow + numpy.
"""
from __future__ import annotations

from collections import deque
from pathlib import Path

import numpy as np
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "static" / "img" / "hero-central-uau.png"
OUT = ROOT / "static" / "img" / "hero-central-uau.png"


def _flood_fill(bg: np.ndarray, seeds: np.ndarray) -> np.ndarray:
    h, w = bg.shape
    vis = np.zeros((h, w), dtype=np.bool_)
    dq: deque[tuple[int, int]] = deque()
    ys, xs = np.nonzero(seeds & bg)
    for y, x in zip(ys.tolist(), xs.tolist(), strict=False):
        vis[y, x] = True
        dq.append((y, x))
    while dq:
        y, x = dq.popleft()
        if y > 0:
            ny, nx = y - 1, x
            if bg[ny, nx] and not vis[ny, nx]:
                vis[ny, nx] = True
                dq.append((ny, nx))
        if y + 1 < h:
            ny, nx = y + 1, x
            if bg[ny, nx] and not vis[ny, nx]:
                vis[ny, nx] = True
                dq.append((ny, nx))
        if x > 0:
            ny, nx = y, x - 1
            if bg[ny, nx] and not vis[ny, nx]:
                vis[ny, nx] = True
                dq.append((ny, nx))
        if x + 1 < w:
            ny, nx = y, x + 1
            if bg[ny, nx] and not vis[ny, nx]:
                vis[ny, nx] = True
                dq.append((ny, nx))
    return vis


def build_transparent_mask(rgb: np.ndarray) -> np.ndarray:
    a = rgb.astype(np.int16)
    r, g, b = a[:, :, 0], a[:, :, 1], a[:, :, 2]
    mx = np.maximum(np.maximum(r, g), b)
    mn = np.minimum(np.minimum(r, g), b)
    chroma = mx - mn
    # Checkerboard in this asset is numerically neutral (corners chroma <= 3).
    bg = (
        (chroma <= 6)
        & (r >= 158)
        & (r <= 222)
        & (g >= 158)
        & (g <= 222)
        & (b >= 158)
        & (b <= 222)
    )
    h, w = bg.shape
    border = np.zeros((h, w), dtype=np.bool_)
    border[0, :] = True
    border[-1, :] = True
    border[:, 0] = True
    border[:, -1] = True
    m = _flood_fill(bg, border)
    cy, cx = h // 2, w // 2
    if bool(bg[cy, cx]):
        center_seed = np.zeros((h, w), dtype=np.bool_)
        center_seed[cy, cx] = True
        m2 = _flood_fill(bg, center_seed)
        m = m | m2
    return m


def main() -> None:
    if not SRC.exists():
        raise SystemExit(f"Missing {SRC}")
    im = Image.open(SRC).convert("RGB")
    rgb = np.array(im)
    trans = build_transparent_mask(rgb)
    red = (
        (rgb[:, :, 0].astype(np.int16) > rgb[:, :, 1] + 15)
        & (rgb[:, :, 0].astype(np.int16) > rgb[:, :, 2] + 15)
        & (rgb[:, :, 0] > 70)
    )
    lost = int((red & trans).sum())
    if lost > 0:
        print("warning: red pixels marked transparent:", lost)
    rgba = np.zeros((rgb.shape[0], rgb.shape[1], 4), dtype=np.uint8)
    rgba[:, :, :3] = rgb
    rgba[:, :, 3] = np.where(trans, 0, 255).astype(np.uint8)
    Image.fromarray(rgba, "RGBA").save(OUT, optimize=True)
    print("wrote", OUT, "transparent pixels:", int(trans.sum()))


if __name__ == "__main__":
    main()
