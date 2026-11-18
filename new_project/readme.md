"""Small original icon set, 24x24 viewBox, stroke-based line icons."""

ICONS = {
    "database": '<path d="M12 3C7.58 3 4 4.34 4 6s3.58 3 8 3 8-1.34 8-3-3.58-3-8-3Z"/><path d="M4 6v6c0 1.66 3.58 3 8 3s8-1.34 8-3V6"/><path d="M4 12v6c0 1.66 3.58 3 8 3s8-1.34 8-3v-6"/>',
    "cloud-upload": '<path d="M7 18a4.5 4.5 0 0 1-.4-8.98 5.5 5.5 0 0 1 10.6-1.5A4.5 4.5 0 0 1 17 18H7Z"/><path d="M12 10v7"/><path d="M9 13l3-3 3 3"/>',
    "bolt": '<path d="M13 2 4 14h6l-1 8 9-12h-6l1-8Z"/>',
    "table": '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M3 9h18"/><path d="M9 4v16"/>',
    "shield-check": '<path d="M12 3 4.5 5.5V11c0 5 3.3 8.3 7.5 10 4.2-1.7 7.5-5 7.5-10V5.5L12 3Z"/><path d="M9 12l2 2 4-4.5"/>',
    "merge": '<circle cx="7" cy="6" r="2"/><circle cx="7" cy="18" r="2"/><circle cx="17" cy="18" r="2"/><path d="M7 8v4a4 4 0 0 0 4 4h4"/><path d="M7 8v8"/>',
    "bar-chart": '<path d="M4 20V10"/><path d="M12 20V4"/><path d="M20 20v-7"/><path d="M4 20h16"/>',
    "calendar": '<rect x="3.5" y="5" width="17" height="16" rx="2"/><path d="M3.5 10h17"/><path d="M8 3v4"/><path d="M16 3v4"/>',
    "lock": '<rect x="5" y="11" width="14" height="10" rx="2"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/>',
    "alert-triangle": '<path d="M12 4 2 20h20L12 4Z"/><path d="M12 10v5"/><circle cx="12" cy="18" r="0.6" fill="currentColor"/>',
    "id-badge": '<rect x="4" y="4" width="16" height="17" rx="2"/><circle cx="12" cy="10" r="2.3"/><path d="M8 17.5c0-2 1.8-3.3 4-3.3s4 1.3 4 3.3"/><path d="M9 4V2.6h6V4"/>',
    "home": '<path d="M4 11.5 12 4l8 7.5"/><path d="M6 10v9.5a1 1 0 0 0 1 1h3.5V16a1.5 1.5 0 0 1 1.5-1.5v0A1.5 1.5 0 0 1 13.5 16v4.5H17a1 1 0 0 0 1-1V10"/>',
    "credit-card": '<rect x="2.5" y="5.5" width="19" height="13" rx="2"/><path d="M2.5 9.5h19"/><path d="M6 14.5h5"/>',
    "search": '<circle cx="10.5" cy="10.5" r="6.5"/><path d="M19.5 19.5 15.2 15.2"/>',
    "layers": '<path d="M12 3 3 8l9 5 9-5-9-5Z"/><path d="M3 13l9 5 9-5"/>',
    "check-circle": '<circle cx="12" cy="12" r="9"/><path d="M8 12.3l2.6 2.6L16 9.3"/>',
    "folder": '<path d="M3 6.5A1.5 1.5 0 0 1 4.5 5h4l2 2.2h9A1.5 1.5 0 0 1 21 8.7v9.3A1.5 1.5 0 0 1 19.5 19.5h-15A1.5 1.5 0 0 1 3 18V6.5Z"/>',
    "cpu": '<rect x="7" y="7" width="10" height="10" rx="1.5"/><rect x="2.8" y="10" width="2.2" height="4"/><rect x="19" y="10" width="2.2" height="4"/><rect x="10" y="2.8" width="4" height="2.2"/><rect x="10" y="19" width="4" height="2.2"/>',
    "info": '<circle cx="12" cy="12" r="9"/><path d="M12 11v5.5"/><circle cx="12" cy="7.8" r="0.6" fill="currentColor"/>',
    "book": '<path d="M4 5.5A1.5 1.5 0 0 1 5.5 4H12v16H5.5A1.5 1.5 0 0 1 4 18.5v-13Z"/><path d="M20 5.5A1.5 1.5 0 0 0 18.5 4H12v16h6.5a1.5 1.5 0 0 0 1.5-1.5v-13Z"/>',
    "list": '<path d="M9 6h11"/><path d="M9 12h11"/><path d="M9 18h11"/><circle cx="4.5" cy="6" r="1"/><circle cx="4.5" cy="12" r="1"/><circle cx="4.5" cy="18" r="1"/>',
    "arrow-right": '<path d="M4 12h15"/><path d="M14 6l6 6-6 6"/>',
    "arrow-down": '<path d="M12 4v15"/><path d="M6 14l6 6 6-6"/>',
    "target": '<circle cx="12" cy="12" r="8.5"/><circle cx="12" cy="12" r="4.7"/><circle cx="12" cy="12" r="0.9" fill="currentColor"/>',
    "ruler": '<path d="M4.5 15.5 15.5 4.5l4 4L8.5 19.5l-4-4Z"/><path d="M9.5 10.5l1.4 1.4"/><path d="M12.2 7.8l1.4 1.4"/>',
    "flag": '<path d="M6 3v18"/><path d="M6 4.5h11l-2.5 3.5L17 11.5H6"/>',
    "map-pin": '<path d="M12 21s7-6.3 7-11.5A7 7 0 0 0 5 9.5C5 14.7 12 21 12 21Z"/><circle cx="12" cy="9.5" r="2.3"/>',
}


def icon(name, size=18, color="currentColor", stroke_width=1.8, css_class=""):
    body = ICONS[name]
    return (f'<svg class="icon {css_class}" width="{size}" height="{size}" viewBox="0 0 24 24" '
            f'fill="none" stroke="{color}" stroke-width="{stroke_width}" '
            f'stroke-linecap="round" stroke-linejoin="round" xmlns="http://www.w3.org/2000/svg">{body}</svg>')
