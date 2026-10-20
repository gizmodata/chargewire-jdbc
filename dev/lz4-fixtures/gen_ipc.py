import sys
import pyarrow as pa
import pyarrow.ipc as ipc

M = 2**64


def splitmix(k):
    z = (k + 0x9E3779B97F4A7C15) % M
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) % M
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) % M
    return z ^ (z >> 31)


def table(first, rows):
    i = list(range(first, first + rows))
    return pa.table({
        "i": pa.array(i, type=pa.int64()),
        "d": pa.array([(splitmix(k) >> 11) / 2**53 for k in i], type=pa.float64()),
        "s": pa.array([None if k % 7 == 0 else f"s{k * 31 % 1000}" for k in i], type=pa.string()),
    })

for codec in ("lz4", "zstd"):
    batches = [table(first=b * 2000, rows=2000).to_batches()[0] for b in range(3)]
    with pa.OSFile(f"{sys.argv[1]}/pyarrow-{codec}.arrows", "wb") as f:
        with ipc.new_stream(sink=f, schema=batches[0].schema, options=ipc.IpcWriteOptions(compression=codec)) as w:
            for batch in batches:
                w.write_batch(batch)
