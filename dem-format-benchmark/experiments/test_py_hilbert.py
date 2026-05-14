import math
def hilbert_encode(x, y):
    d = 0
    for i in range(31, -1, -1):
        rx = (x >> i) & 1
        ry = (y >> i) & 1
        quad = (3 * rx) ^ ry
        d += quad << (2 * i)
        if ry == 0:
            if rx == 1:
                mask = 0xFFFFFFFF if i == 31 else (1 << (i + 1)) - 1
                x = x ^ mask
                y = y ^ mask
            x, y = y, x
    return d

def _zorder_cells_for_bbox(bbox):
    col_min = int(round((bbox[0] + 180.0) * 3600.0))
    col_max = int(round((bbox[2] + 180.0) * 3600.0))
    row_max = int(round((90.0 - bbox[1]) * 3600.0))
    row_min = int(round((90.0 - bbox[3]) * 3600.0))
    if col_min > col_max: col_min, col_max = col_max, col_min
    if row_min > row_max: row_min, row_max = row_max, row_min
    print(col_min, col_max, row_min, row_max)
    expanded = set()
    for c in range(col_min, col_max + 1):
        for r in range(row_min, row_max + 1):
            d = hilbert_encode(c, r)
            for lvl in range(16):
                expanded.add((d >> (2 * lvl)) << 4 | lvl)
    return list(expanded)

print(len(_zorder_cells_for_bbox([8.5, 46.5, 8.51, 46.51])))
