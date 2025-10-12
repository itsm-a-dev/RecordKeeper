# utils/graphics.py
import io
import os
import math
from typing import List, Sequence
from datetime import date
from decimal import Decimal

from PIL import Image, ImageDraw, ImageFont
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# You can install a font into repo or default to system fonts;
# On Railway default fonts exist, but you may include a TTF in repo and reference it here.
DEFAULT_FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

def create_line_chart(dates: Sequence[date], net_units: Sequence[float], width=800, height=300) -> bytes:
    """
    Create a simple line chart and return PNG bytes.
    """
    fig, ax = plt.subplots(figsize=(width/100, height/100), dpi=100)
    ax.plot(dates, net_units, linewidth=2)
    ax.axhline(0, color="grey", linewidth=0.7)
    ax.set_xlabel("Date")
    ax.set_ylabel("Net Units")
    fig.autofmt_xdate(rotation=30)
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf.read()

def create_recap_card(title: str, subtitle: str, stats: dict, mini_chart_bytes: bytes = None, width=1024, height=512) -> bytes:
    """
    Create a branded recap card as PNG bytes.
    - stats: dict of display labels to values
    - mini_chart_bytes: png bytes for a small chart to paste in
    """
    bg = Image.new("RGBA", (width, height), (18, 18, 20, 255))
    draw = ImageDraw.Draw(bg)

    try:
        font_title = ImageFont.truetype(DEFAULT_FONT, 40)
        font_sub = ImageFont.truetype(DEFAULT_FONT, 20)
        font_stat_label = ImageFont.truetype(DEFAULT_FONT, 18)
        font_stat_val = ImageFont.truetype(DEFAULT_FONT, 28)
    except Exception:
        font_title = ImageFont.load_default()
        font_sub = ImageFont.load_default()
        font_stat_label = ImageFont.load_default()
        font_stat_val = ImageFont.load_default()

    # Title
    draw.text((40, 30), title, font=font_title, fill=(255, 255, 255, 255))
    draw.text((40, 80), subtitle, font=font_sub, fill=(200, 200, 200, 255))

    # Stats pane
    x = 40
    y = 140
    for label, val in stats.items():
        draw.text((x, y), f"{label}", font=font_stat_label, fill=(170, 170, 170, 255))
        draw.text((x, y+24), f"{val}", font=font_stat_val, fill=(255, 255, 255, 255))
        x += 220
        if x > width - 200:
            x = 40
            y += 80

    # mini chart
    if mini_chart_bytes:
        try:
            chart = Image.open(io.BytesIO(mini_chart_bytes)).convert("RGBA")
            # resize maintain aspect
            chart = chart.resize((int(width*0.45), int(height*0.45)))
            bg.paste(chart, (width - chart.width - 40, height - chart.height - 40), chart)
        except Exception:
            pass

    buf = io.BytesIO()
    bg.save(buf, format="PNG")
    buf.seek(0)
    return buf.read()
