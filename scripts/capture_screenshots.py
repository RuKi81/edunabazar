"""Capture screenshots from the Agrocosmos production portal for the presentation.

Targets:
    01_dashboard_all_russia.png   /agrocosmos/?region=all
    02_region_view.png            /agrocosmos/?region=<region>
    03_district_ndvi_chart.png    /agrocosmos/?region=<region>&district=<district>
    04_raster_dashboard.png       /agrocosmos/raster-dashboard/?region=<region>&district=<district>

Output dir: presentation/screens/

Run:
    py scripts/capture_screenshots.py
"""
from __future__ import annotations

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright


BASE = 'https://edunabazar.ru'
OUT = Path(__file__).resolve().parent.parent / 'presentation' / 'screens'
OUT.mkdir(parents=True, exist_ok=True)

# Use a tested region/district: Крым (37) / Бахчисарайский
REGION_ID = '37'

VIEWPORT = {'width': 1600, 'height': 1000}


def wait_map(page, timeout_ms=20000):
    """Wait for Leaflet map + first overlay to render. Best-effort."""
    try:
        page.wait_for_selector('#agro-map', state='attached', timeout=timeout_ms)
    except Exception as e:
        print(f'    (wait_map: #agro-map not found, continuing) {e!s:.120}')
    try:
        page.wait_for_load_state('networkidle', timeout=timeout_ms)
    except Exception:
        pass
    page.wait_for_timeout(1500)


def pick_district(page, name_substr: str | None = None) -> str | None:
    """Return a district id from <select id="agro-district">.

    If ``name_substr`` is given (case-insensitive), pick the option whose
    text contains it. Otherwise return the first non-empty option.
    """
    return page.evaluate(
        """(needle) => {
            const sel = document.querySelector('select#agro-district');
            if (!sel) return null;
            const lc = needle ? needle.toLowerCase() : null;
            if (lc) {
                for (const o of sel.options) {
                    if (o.value && o.text.toLowerCase().includes(lc)) {
                        return o.value;
                    }
                }
            }
            for (const o of sel.options) {
                if (o.value && o.value !== '') return o.value;
            }
            return null;
        }""",
        name_substr,
    )


def shot(page, name):
    path = OUT / name
    page.screenshot(path=str(path), full_page=False)
    print(f'  saved {path.name}  ({path.stat().st_size // 1024} KB)')


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(
            viewport=VIEWPORT,
            device_scale_factor=2,
            locale='ru-RU',
            user_agent=(
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            ),
        )
        page = ctx.new_page()
        page.set_default_navigation_timeout(90_000)
        page.set_default_timeout(30_000)

        # 1) Карта всей России
        url = f'{BASE}/agrocosmos/?region=all'
        print(f'[1/4] {url}')
        page.goto(url, wait_until='domcontentloaded')
        wait_map(page)
        # Allow the country choropleth to finish painting (3-7s typical)
        page.wait_for_timeout(4000)
        shot(page, '01_dashboard_all_russia.png')

        # 2) Регион с NDVI-графиком (Республика Крым)
        url = f'{BASE}/agrocosmos/?region={REGION_ID}'
        print(f'[2/5] {url}')
        page.goto(url, wait_until='domcontentloaded')
        wait_map(page)
        try:
            page.wait_for_selector('canvas', timeout=10000)
        except Exception:
            pass
        page.wait_for_timeout(4000)
        shot(page, '02_region_ndvi_chart.png')

        # Discover the Нижнегорский district id (has S2/L8 NDVI for 2026/2025)
        did = pick_district(page, 'Нижнегорский') or pick_district(page) or ''
        print(f'    picked district_id={did!r}')

        # 3) Reports page (региональный отчёт)
        url = f'{BASE}/agrocosmos/report/region/?region={REGION_ID}'
        print(f'[3/5] {url}')
        page.goto(url, wait_until='domcontentloaded')
        try:
            page.wait_for_load_state('networkidle', timeout=20000)
        except Exception:
            pass
        page.wait_for_timeout(4000)
        # Reports page may be long — capture upper viewport
        shot(page, '03_report_region.png')

        # 4) Растровый дашборд — Нижнегорский с включённым NDVI растровым слоем
        url = f'{BASE}/agrocosmos/raster/?region={REGION_ID}'
        print(f'[4/5] {url}')
        page.goto(url, wait_until='domcontentloaded')
        wait_map(page)
        # Re-pick the district in the raster page (IDs may differ)
        rdid = pick_district(page, 'Нижнегорский') or did
        print(f'    raster district_id={rdid!r}')
        # Programmatically select district, enable borders + raster overlay
        try:
            page.evaluate(
                """(dist_id) => {
                    const sel = document.getElementById('agro-district');
                    if (sel && dist_id) {
                        sel.value = String(dist_id);
                        sel.dispatchEvent(new Event('change'));
                    }
                    function tick(id) {
                        const cb = document.getElementById(id);
                        if (cb && !cb.checked) {
                            cb.checked = true;
                            cb.dispatchEvent(new Event('change'));
                        }
                    }
                    tick('agro-toggle-borders');
                    tick('agro-toggle-raster');
                }""",
                rdid,
            )
        except Exception as e:
            print(f'    (toggle overlays failed) {e!s:.120}')
        # Raster pipeline: composites fetch (~3s) + PNG tile load (~5-10s)
        page.wait_for_timeout(15000)
        # Click on the map to close any open dropdown
        try:
            page.evaluate("document.body.click();")
            page.wait_for_timeout(500)
        except Exception:
            pass
        shot(page, '04_raster_dashboard.png')

        # 5) Тот же дашборд, но с zoom-in на растровые тайлы Нижнегорского
        # (Leaflet map instance is in a closure, so use mouse wheel)
        try:
            # Tiles appeared in eastern Crimea around viewport (~720, 290)
            page.mouse.move(720, 290)
            for _ in range(3):
                page.mouse.wheel(0, -400)
                page.wait_for_timeout(700)
            page.wait_for_timeout(6000)
            shot(page, '05_raster_zoomed.png')
        except Exception as e:
            print(f'    (zoom failed) {e!s:.120}')

        browser.close()

    print()
    print(f'Done. Screens in: {OUT}')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
