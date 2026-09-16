@echo off
REM Select Playwright MCP backend for this cmd session.
REM Requires Node.js 20+ (npx) and Chrome CDP on 9222.

set "PATH=%ProgramFiles%\nodejs;%PATH%"
set BROWSER_DRIVER_BACKEND=playwright_mcp
set BROWSER_DRIVER=remote
set PLAYWRIGHT_CDP_URL=http://127.0.0.1:9222
set BROWSER_CDP_URL=http://127.0.0.1:9222
set PLAYWRIGHT_MCP_COMMAND=npx
set PLAYWRIGHT_MCP_ARGS=-y @playwright/mcp@latest

echo BROWSER_DRIVER_BACKEND=%BROWSER_DRIVER_BACKEND%
echo PLAYWRIGHT_CDP_URL=%PLAYWRIGHT_CDP_URL%
where node
where npx
node -v
npx -v
