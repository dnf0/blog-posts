# Gemini CLI Bug Report Context: PTY Session Exhaustion

**Issue Summary:**
Excessive PTY session consumption and process leaking in Gemini CLI, leading to resource exhaustion. The issue persists despite recent fixes in v0.39.0 - v0.41.0 regarding PTY exhaustion and handler leaks.

**Environment Details:**
*   **OS:** macOS (darwin)
*   **Gemini CLI Version:** 0.42.0
*   **Interactive Shell:** Enabled (`tools.shell.enableInteractiveShell = true`)
*   **External Hook/Proxy:** The environment uses **RTK (Rust Token Killer) v0.37.1** to proxy and intercept commands. 

**Technical Context & Investigation:**
1.  **Symptom:** PTY sessions (`node-pty`) are not being cleaned up properly after command execution, leading to a buildup of background processes.
2.  **User Constraint:** The user relies heavily on interactive terminal prompts, so setting `enableInteractiveShell: false` is not a viable workaround.
3.  **Hypothesis:** The `rtk` wrapper, which intercepts `run_shell_command` outputs to filter tokens, may be preventing Gemini CLI from correctly detecting process completion or `stdout`/`stderr` inactivity. This could be causing the `inactivityTimeout` to fail or the `node-pty` session to become orphaned.
4.  **Prior Fixes:** We reviewed the changelogs and noted that PRs #24752, #25079 (v0.39.0), #24397 (v0.40.0), and #26065 (v0.41.0-preview.0) addressed similar handler leaks and PTY exhaustion. Since this environment is on v0.42.0, this represents an unresolved edge case or regression, likely tied to the interaction between the CLI's PTY lifecycle and external tool hooks.

**Suggested Reproduction Steps:**
1. Configure an external proxy hook (like `rtk`) in `~/.gemini/gemini.md`.
2. Run standard commands that get intercepted.
3. Monitor the active PTY sessions and note the failure to clean up orphaned processes.