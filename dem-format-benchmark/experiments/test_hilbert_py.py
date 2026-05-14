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
print(hilbert_encode(0,0))
print(hilbert_encode(1,0))
print(hilbert_encode(2,0))
