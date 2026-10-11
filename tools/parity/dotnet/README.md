# IBM .NET parity worker

Oracle-only worker using IBM's `Net.IBM.Data.Db2` provider packages. IBM ships one NuGet package per OS (`Net.IBM.Data.Db2` on Windows, `-lnx` on Linux, `-osx` on macOS, all pinned to the same version in `Parity.csproj`); `packages.lock.json` is committed and restored in locked mode. Because the package reference is OS-conditional, the committed lock file reflects the Linux package; regenerate it on the target OS if you run the worker elsewhere. The current IBM package targets .NET 10. On Linux the deployed `clidriver/lib` must be on `LD_LIBRARY_PATH` (the parity script sets this); otherwise the provider fails with `DllNotFoundException`.

From the repository root, with `DB2_TEST_DSN` set:

```sh
dotnet restore tools/parity/dotnet/Parity.csproj --locked-mode
dotnet run --no-restore --project tools/parity/dotnet/Parity.csproj -- --all
dotnet run --no-restore --project tools/parity/dotnet/Parity.csproj -- --case decimal_31_10
```

It consumes the public shared SQL manifest and writes one canonical JSON object per line to stdout. It has no dependency on the Rust crate.
