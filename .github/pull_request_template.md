## What is the intent of this PR?

## Checklist
- [ ] all tests complete. Make sure you have `-DTESTING=1` and `make test` completes.
- [ ] code is formatted. This includes ALL code. For now, this can be done with:
```bash
mkdir -p build && cd build
cmake -DTESTING=1 -DSZD_TOOLS="szdcli; reset_perf" ..
make format
```
