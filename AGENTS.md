# Repository Agent Notes

- Playwright is available in the current Codex environment through the bundled runtime. Do not report it as missing without checking the bundled runtime first.
- The bundled package is `playwright@1.62.1` under `C:\Users\zxc\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\node_modules`.
- This repository currently has no root `package.json` or local `node_modules`. The smoke test imports `@playwright/test`, while the bundled runtime exposes `playwright`; treat a runner import mismatch as a project configuration issue, not an environment absence.
