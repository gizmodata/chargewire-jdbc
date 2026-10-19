import sys
M = 2**64
def lcg(x): return (x * 6364136223846793005 + 1442695040888963407) % M
def text(lines, seed=1):
    x = seed; out = []
    for i in range(lines):
        x = lcg(x)
        out.append(f"row {i:06d} status={'OK' if (x >> 33) % 4 else 'FAIL'} value={(x >> 40) % 100000}\n")
    return "".join(out).encode()
def noise(n, seed=2):
    x = seed; out = bytearray()
    for _ in range(n):
        x = lcg(x); out.append(x >> 56)
    return bytes(out)
open(sys.argv[1] + "/text.bin", "wb").write(text(3000))
open(sys.argv[1] + "/noise.bin", "wb").write(noise(70000))
