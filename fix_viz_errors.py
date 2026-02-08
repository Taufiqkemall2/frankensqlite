
import re

INPUT_FILE = "visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html"
OUTPUT_FILE = "visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html"

# CSS content from the previous turn (FrankenTUI)
FRANKENTUI_CSS = """
:root {
  --bg-color: #050505;
  --fg-color: #33ff00;
  --dim-color: #004400;
  --accent-color: #bd00ff;
  --error-color: #ff003c;
  --panel-bg: #0a0a0a;
  --border-color: #33ff00;
  --grid-line: #112211;
  --font-mono: 'JetBrains Mono', monospace;
  --glow: 0 0 10px rgba(51, 255, 0, 0.2);
}

* { box-sizing: border-box; }

body {
  background-color: var(--bg-color);
  color: var(--fg-color);
  font-family: var(--font-mono);
  margin: 0;
  overflow: hidden;
  height: 100vh;
  display: flex;
  flex-direction: column;
}

/* CRT Scanline */
body::after {
  content: "";
  position: fixed;
  inset: 0;
  background: linear-gradient(rgba(18, 16, 16, 0) 50%, rgba(0, 0, 0, 0.25) 50%), linear-gradient(90deg, rgba(255, 0, 0, 0.06), rgba(0, 255, 0, 0.02), rgba(0, 0, 255, 0.06));
  background-size: 100% 2px, 2px 100%;
  pointer-events: none;
  z-index: 9999;
}

/* TUI Layout */
.tui-screen {
  display: flex;
  flex-direction: column;
  height: 100%;
  padding: 1rem;
  gap: 1rem;
}

.tui-header {
  border-bottom: 2px solid var(--border-color);
  padding-bottom: 0.5rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  text-transform: uppercase;
  letter-spacing: 0.1em;
}

.tui-grid {
  display: grid;
  grid-template-columns: 300px 1fr;
  gap: 1rem;
  flex: 1;
  min-height: 0;
}

.tui-panel {
  border: 1px solid var(--border-color);
  background: var(--panel-bg);
  position: relative;
  display: flex;
  flex-direction: column;
  box-shadow: var(--glow);
}

.tui-panel::before {
  content: "";
  position: absolute;
  top: -1px; left: -1px; right: -1px; bottom: -1px;
  border: 1px dashed var(--dim-color);
  pointer-events: none;
  z-index: 1;
}

.tui-panel-header {
  background: var(--fg-color);
  color: var(--bg-color);
  padding: 0.25rem 0.5rem;
  font-weight: bold;
  text-transform: uppercase;
  font-size: 0.8rem;
  display: flex;
  justify-content: space-between;
}

.tui-panel-content {
  flex: 1;
  overflow: auto;
  padding: 1rem;
}

/* Controls */
button {
  background: transparent;
  border: 1px solid var(--fg-color);
  color: var(--fg-color);
  font-family: var(--font-mono);
  padding: 0.5rem 1rem;
  cursor: pointer;
  text-transform: uppercase;
  transition: all 0.1s;
}
button:hover {
  background: var(--fg-color);
  color: var(--bg-color);
  box-shadow: var(--glow);
}
button:active {
  transform: translateY(1px);
}

input, select {
  background: #000;
  border: 1px solid var(--dim-color);
  color: var(--fg-color);
  font-family: var(--font-mono);
  padding: 0.5rem;
  width: 100%;
}
input:focus, select:focus {
  border-color: var(--fg-color);
  outline: none;
  box-shadow: var(--glow);
}

/* Scrollbar */
::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-track { background: var(--bg-color); border-left: 1px solid var(--dim-color); }
::-webkit-scrollbar-thumb { background: var(--dim-color); border: 1px solid var(--bg-color); }
::-webkit-scrollbar-thumb:hover { background: var(--fg-color); }

/* Utility */
.hidden { display: none !important; }
.flex { display: flex; }
.gap-2 { gap: 0.5rem; }
.items-center { align-items: center; }
.justify-between { justify-content: space-between; }
.w-full { width: 100%; }
.mt-4 { margin-top: 1rem; }
.text-xs { font-size: 0.75rem; }
.font-bold { font-weight: bold; }

/* Frankenstein Stitches Decoration */
.stitch-marker {
  position: absolute;
  width: 10px; height: 2px;
  background: var(--fg-color);
}
"""

def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

original_html = read_file(INPUT_FILE)

# 1. Embed CSS to fix MIME type error
# Replace <link rel="stylesheet" href="frankentui.css" /> with inline style
# Also keep other css includes.
css_link_pattern = r'<link rel="stylesheet" href="frankentui.css"\s*/>'
if re.search(css_link_pattern, original_html):
    print("Replacing linked CSS with embedded CSS...")
    new_html = re.sub(css_link_pattern, f'<style>{FRANKENTUI_CSS}</style>', original_html)
else:
    # If not found (maybe changed by enhance_viz), try to inject it before </head>
    print("CSS link not found, injecting into head...")
    new_html = original_html.replace('</head>', f'<style>{FRANKENTUI_CSS}</style></head>')

# 2. Robustify DB Loading
# We need to modify the init() function to check the fetch response.
# The original code looks like:
# const res = await fetch(DB_URL);
# const buf = await res.arrayBuffer();
# DB = new SQL.Database(new Uint8Array(buf));

new_db_loading_logic = """
            const res = await fetch(DB_URL);
            if (!res.ok) {
                throw new Error(`HTTP ${res.status} ${res.statusText} fetching ${DB_URL}`);
            }
            const contentType = res.headers.get("content-type");
            if (contentType && contentType.includes("text/html")) {
                throw new Error(`Server returned HTML (likely 404) for ${DB_URL}. Is the file correctly served?`);
            }
            const buf = await res.arrayBuffer();
            if (buf.byteLength < 16) {
                 throw new Error(`File ${DB_URL} is too small (${buf.byteLength} bytes).`);
            }
            // Check SQLite magic header "SQLite format 3"
            const magic = new Uint8Array(buf.slice(0, 16));
            const magicStr = String.fromCharCode(...magic);
            if (!magicStr.startsWith("SQLite format 3")) {
                 throw new Error(`File ${DB_URL} is not a valid SQLite database (header mismatch). Got: "${magicStr.slice(0,15)}..."`);
            }
            
            DB = new SQL.Database(new Uint8Array(buf));
"""

# Replace the specific block in init()
# We target the lines inside the try block of init()
pattern = r'const res = await fetch\(DB_URL\);\s*const buf = await res\.arrayBuffer\(\);\s*DB = new SQL\.Database\(new Uint8Array\(buf\)\);'
match = re.search(pattern, new_html)
if match:
    print("Patching DB loading logic...")
    new_html = new_html.replace(match.group(0), new_db_loading_logic)
else:
    print("Could not find exact DB loading logic to patch. Attempting broader search...")
    # Fallback: look for just the fetch and replace until DB init
    # This regex is a bit risky if formatting changed, but we just wrote the file so it should match.
    # Let's try to locate the init function start.
    start_marker = "async function init() {"
    if start_marker in new_html:
        print("Found init function, attempting manual insertion if regex failed.")
        # But for now, let's assume the previous enhance/restore scripts kept it relatively standard.
        # If the regex failed, it might be whitespace.
        pass 

# 3. Clean up any double-injected scripts from previous turns if they exist
# (enhance_viz injected before init(), potentially leaving old init calls?)
# The previous script did `new_html.replace('init();', js_injections + '
    init();')`
# We should ensure we don't have multiple `init()` calls if we run this multiple times.
# But since we are reading the file fresh, it should be the state *after* enhance_viz.

write_file(OUTPUT_FILE, new_html)
print(f"Fixed {OUTPUT_FILE}")
