"""
Dynamic 1586×672 community welcome banner (Pillow).
Base art: banner_base.jpg or banner_base.png next to this file.
"""
from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path
from typing import Optional, Union

from PIL import Image, ImageDraw, ImageFont

CANVAS_W, CANVAS_H = 1586, 672
PFP_SIZE = 280
PFP_CENTER = (488, 495)
PFP_TOP_LEFT = (PFP_CENTER[0] - PFP_SIZE // 2, PFP_CENTER[1] - PFP_SIZE // 2)

# Text block: upper-right, same left edge — large Creepster/Chiller-style type (clear of 6XS logo)
USER_TEXT_LEFT_X = 698
USER_TEXT_TOP_Y = 228
USER_FONT_PX = 118
USER_STROKE = 4

MEMBER_FONT_PX = 76
MEMBER_STROKE = 3
TEXT_LINE_GAP = 20

RED_OUTLINE = (165, 0, 0)  # #A50000
FILL = (0, 0, 0)

_SCRIPT_DIR = Path(__file__).resolve().parent


def _find_base_image() -> Path:
    for name in ("banner_base.jpg", "banner_base.jpeg", "banner_base.png"):
        p = _SCRIPT_DIR / name
        if p.is_file():
            return p
    raise FileNotFoundError(
        f"Place banner_base.jpg or banner_base.png in {_SCRIPT_DIR} (1586×672 artwork)."
    )


def _banner_font_paths() -> list[Path]:
    """Prefer bundled / horror display fonts so we never fall back to tiny bitmap default."""
    win = Path(os.environ.get("WINDIR", "C:\\Windows")) / "Fonts"
    return [
        _SCRIPT_DIR / "fonts" / "Creepster-Regular.ttf",
        _SCRIPT_DIR / "fonts" / "Nosifer-Regular.ttf",
        win / "CHILLER.TTF",
        win / "chiller.ttf",
        win / "JOKERMAN.TTF",
        _SCRIPT_DIR / "BebasNeue-Regular.ttf",
        _SCRIPT_DIR / "fonts" / "BebasNeue-Regular.ttf",
        win / "impact.ttf",
        Path("/usr/share/fonts/truetype/msttcorefonts/Impact.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
    ]


def _load_font(size_px: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for p in _banner_font_paths():
        try:
            if p.is_file():
                return ImageFont.truetype(str(p), size=size_px)
        except OSError:
            continue
    return ImageFont.load_default()


def _member_id_digits(member_count: int) -> str:
    """4-digit display (zfill); longer counts use last four digits."""
    s = str(max(0, int(member_count)))
    if len(s) <= 4:
        return s.zfill(4)
    return s[-4:]


def _circle_avatar(pfp: Image.Image, size: int) -> Image.Image:
    pfp = pfp.convert("RGBA")
    pfp = pfp.resize((size, size), Image.Resampling.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((0, 0, size - 1, size - 1), fill=255)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    out.paste(pfp, (0, 0), mask)
    return out


def _truncate_username(raw: str, max_chars: int = 18) -> str:
    u = (raw or "USER").upper()
    if len(u) <= max_chars:
        return u
    return u[: max_chars - 1] + "…"


def render_welcome_banner(
    username: str,
    member_count: int,
    avatar_source: Union[bytes, Image.Image, Path],
    *,
    base_path: Optional[Path] = None,
) -> bytes:
    """
    Build PNG bytes: circular PFP, USER: line, MEMBER ID: 4-digit line.
    """
    base_p = base_path or _find_base_image()
    base = Image.open(base_p).convert("RGBA")
    if base.size != (CANVAS_W, CANVAS_H):
        base = base.resize((CANVAS_W, CANVAS_H), Image.Resampling.LANCZOS)

    if isinstance(avatar_source, Path):
        pfp_img = Image.open(avatar_source).convert("RGBA")
    elif isinstance(avatar_source, Image.Image):
        pfp_img = avatar_source
    else:
        pfp_img = Image.open(io.BytesIO(avatar_source)).convert("RGBA")

    circ = _circle_avatar(pfp_img, PFP_SIZE)
    base.alpha_composite(circ, PFP_TOP_LEFT)

    draw = ImageDraw.Draw(base)
    font_user = _load_font(USER_FONT_PX)
    font_mem = _load_font(MEMBER_FONT_PX)

    user_line = f"USER: {_truncate_username(username)}"
    mem_line = f"MEMBER ID: {_member_id_digits(member_count)}"

    ux, uy = USER_TEXT_LEFT_X, USER_TEXT_TOP_Y
    draw.text(
        (ux, uy),
        user_line,
        font=font_user,
        fill=FILL,
        stroke_width=USER_STROKE,
        stroke_fill=RED_OUTLINE,
    )
    _l, _t, _r, b = draw.textbbox(
        (ux, uy),
        user_line,
        font=font_user,
        stroke_width=USER_STROKE,
    )
    mem_y = b + TEXT_LINE_GAP
    draw.text(
        (ux, mem_y),
        mem_line,
        font=font_mem,
        fill=FILL,
        stroke_width=MEMBER_STROKE,
        stroke_fill=RED_OUTLINE,
    )

    buf = io.BytesIO()
    base.convert("RGB").save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def main() -> None:
    parser = argparse.ArgumentParser(description="Render a sample community banner PNG.")
    parser.add_argument("username", nargs="?", default="TESTUSER", help="Display name (uppercased)")
    parser.add_argument("--count", type=int, default=78, help="Guild member count (4-digit display)")
    parser.add_argument("--avatar", type=Path, help="Local image for PFP test")
    parser.add_argument("-o", "--output", type=Path, default=Path("banner_out.png"))
    args = parser.parse_args()

    if args.avatar and args.avatar.is_file():
        av: Union[bytes, Image.Image, Path] = args.avatar
    else:
        # 1×1 placeholder if no avatar file
        placeholder = Image.new("RGB", (64, 64), (40, 40, 40))
        b = io.BytesIO()
        placeholder.save(b, format="PNG")
        av = b.getvalue()

    try:
        png = render_welcome_banner(args.username, args.count, av)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    args.output.write_bytes(png)
    print(f"Wrote {args.output.resolve()} ({len(png)} bytes)")


if __name__ == "__main__":
    main()
