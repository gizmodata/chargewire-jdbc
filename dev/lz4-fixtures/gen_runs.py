import sys
out = bytearray()
for period in range(1, 21):
    pattern = bytes((period * 7 + k) % 256 for k in range(period))
    out += (pattern * (3000 // period + 1))[:3000]
open(sys.argv[1] + "/runs.bin", "wb").write(bytes(out))
