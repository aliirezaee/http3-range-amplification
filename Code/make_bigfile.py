"""Run once to regenerate bigfile.bin (10 240 bytes of 0x41)."""
import os
out = os.path.join(os.path.dirname(__file__), "bigfile.bin")
with open(out, "wb") as f:
    f.write(b"A" * 10240)
print(f"Created {out} ({os.path.getsize(out)} bytes)")
